package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

var fetchingFeedsWg sync.WaitGroup

var associatingMediaWg sync.WaitGroup

type AppConfig struct {
	Db DbConfig `json:"db"`
	Solr string `json:"solr"`
}

type DbConfig struct {
	User string `json:"user"`
	Password string `json:"pass"`
	Server string `json:"server"`
	DbName string `json:"dbName"`
}

type Site struct {
	siteId int
	feedName string
	feedUrl string
	siteType string
	active int
	altName string
	created string
	modified string
	lastChecked string
	daysSinceLastPost int
	posts []Post
}

type Post struct {
	postTitle string
	pubDate string
	link string
	description string
	content string
	categories []string
	MediaIds []int64
}

type RssDoc struct {
	XMLName xml.Name `xml:"rss"`
	Channel RssChannel `xml:"channel"`
}

type RssChannel struct {
	XMLName xml.Name `xml:"channel"`
	Title string `xml:"title"`
	Items []RssItem `xml:"item"`
}

type RssItem struct {
	XMLName xml.Name `xml:"item"`
	Title string `xml:"title"`
	Link string `xml:"link"`
	PubDate string `xml:"pubDate"`
	Description string `xml:"description"`
	Content string `xml:"encoded"`
	Categories []string `xml:"category"`
}

type YouTubeFeed struct {
	XMLName xml.Name `xml:"feed"`
	Title string `xml:"title"`
	Entries []YouTubeEntry `xml:"entry"`
}

type YouTubeEntry struct {
	XMLName xml.Name `xml:"entry"`
	Title string `xml:"title"`
	Published string `xml:"published"`
	Link YouTubeEntryLink `xml:"link"`
	MediaGroup YouTubeEntryMediaGroup `xml:"group"`
}

type YouTubeEntryLink struct {
	Href string `xml:"href,attr"`
}

type YouTubeEntryMediaGroup struct {
	XMLName xml.Name `xml:"group"`
	Description string `xml:"description"`
}

type SiteResponse struct {
	site Site
	hasRssDoc bool
	rssDoc RssDoc
	youTubeFeed YouTubeFeed
}

type MediaItem struct {
	MediaID int64
	Title string
}

type MediaMatched struct {
	SiteKey int
	PostKey int
	Post Post
	MediaItems map[int64]string
}

func getFeeds(db *sql.DB) []Site {
	getFeedsRows, err := db.Query("SELECT * FROM sites WHERE active = 1")
	if err != nil {
		panic(err)
	}

	defer func(getFeedsRows *sql.Rows) {
		err := getFeedsRows.Close()
		if err != nil {
			panic(err)
		}
	}(getFeedsRows)

	sitesToFetch := make([]Site, 0)

	for getFeedsRows.Next() {
		rssFeed := Site{}
		err = getFeedsRows.Scan(
			&rssFeed.siteId,
			&rssFeed.feedName,
			&rssFeed.feedUrl,
			&rssFeed.siteType,
			&rssFeed.active,
			&rssFeed.altName,
			&rssFeed.created,
			&rssFeed.modified,
			&rssFeed.lastChecked,
			&rssFeed.daysSinceLastPost,
		)
		if err != nil {
			panic(err)
		}

		sitesToFetch = append(sitesToFetch, rssFeed)
	}

	return sitesToFetch
}

func closeRssFeed(parsedFeed *SiteResponse, rssFeedChan chan<- SiteResponse) {
	rssFeedChan <- *parsedFeed
	fetchingFeedsWg.Done()
}

func updateSiteData(parsedFeed *SiteResponse)  {
	if parsedFeed.site.siteType == "Anitube" {
		parsedFeed.site.feedName = parsedFeed.youTubeFeed.Title

		for _, entry := range parsedFeed.youTubeFeed.Entries {
			pubDate, err := time.Parse("2006-01-02T15:04:05-07:00", entry.Published)
			if err != nil {
				pubDate = time.Now()
			}

			var post = Post{
				postTitle:   	entry.Title,
				pubDate:     	pubDate.UTC().Format("2006-01-02 15:04:05"),
				link:        	entry.Link.Href,
				description: 	entry.MediaGroup.Description,
			}

			parsedFeed.site.posts = append(parsedFeed.site.posts, post)
		}
	} else {
		parsedFeed.site.feedName = parsedFeed.rssDoc.Channel.Title

		for _, item := range parsedFeed.rssDoc.Channel.Items {
			pubDate, err := time.Parse("Mon, 02 Jan 2006 15:04:05 -0700", item.PubDate)

			if err != nil {
				pubDate, err = time.Parse("Mon, 2 Jan 2006 15:04:05 -0700", item.PubDate)

				if err != nil {
					pubDate, err = time.Parse("Mon, 02 Jan 2006 15:04:05 MST", item.PubDate)

					if err != nil {
						pubDate = time.Now()
					}
				}
			}

			var post = Post{
				postTitle:   	item.Title,
				pubDate:     	pubDate.UTC().Format("2006-01-02 15:04:05"),
				link:        	item.Link,
				description: 	item.Description,
				content: 		item.Content,
				categories:  	item.Categories,
			}

			if len(post.postTitle) > 150 {
				continue
			}

			parsedFeed.site.posts = append(parsedFeed.site.posts, post)
		}
	}
}

func getSiteFeed(httpClient *http.Client, site Site, rssFeedChan chan<- SiteResponse) {
	var rssDoc RssDoc
	var youTubeFeed YouTubeFeed

	parsedFeed := SiteResponse{
		site: site,
		hasRssDoc: false,
		rssDoc: rssDoc,
		youTubeFeed: youTubeFeed,
	}

	defer closeRssFeed(&parsedFeed, rssFeedChan)

	req, err := http.NewRequest("GET", site.feedUrl, nil)
	if err != nil {
		fmt.Println("Could not create new request for ", site.feedName)
		return
	}

	// tumblr gdpr nonsense
	if !strings.Contains(site.feedUrl, "tumblr.com") {
		req.Header.Add("User-Agent", "@bateszi RSS feed parser")
	} else {
		req.Header.Add("User-Agent", "Baiduspider")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)

	defer func(cancel context.CancelFunc) {
		cancel()
	}(cancel)

	req = req.WithContext(ctx)

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer func(resp *http.Response) {
		_ = resp.Body.Close()
	}(resp)

	if resp.StatusCode == http.StatusOK && resp.StatusCode < 300 {
		httpBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Could not read response from: ", site.feedName, err.Error())
		}

		if site.siteType == "Anitube" {
			err = xml.Unmarshal(httpBody, &parsedFeed.youTubeFeed)
		} else {
			err = xml.Unmarshal(httpBody, &parsedFeed.rssDoc)
		}

		if err != nil {
			fmt.Println("Could not parse XML from ", site.feedUrl, err.Error())
		}

		fmt.Println("Retrieved RSS feed", parsedFeed.site.feedUrl)
		parsedFeed.hasRssDoc = true

		updateSiteData(&parsedFeed)
	}
}

func updateSiteInfo(db *sql.DB, response SiteResponse) bool {
	stmt, err := db.Prepare("UPDATE sites SET feed_name = ?, last_checked = ? WHERE pk_site_id = ?")
	if err != nil {
		fmt.Println("Could not prepare SQL statement", response.site.feedUrl, err.Error())
		return false
	}

	res, err := stmt.Exec(
		response.site.feedName,
		getCurrentDateTimeUtc(),
		response.site.siteId,
	)
	if err != nil {
		fmt.Println("Could not execute SQL statement", response.site.feedUrl, err.Error())
		return false
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		fmt.Println(
			"Could not retrieve number of rows affected by SQL statement",
			response.site.feedUrl,
			err.Error(),
		)
		return false
	}

	fmt.Println("Updated", response.site.feedName, response.site.altName)
	return rowsAffected == 1
}

func updateSiteDaysSinceStat(db *sql.DB, response SiteResponse) bool {
	var latestPost string
	err := db.QueryRow(
		"SELECT pub_date FROM posts WHERE fk_site_id = ? ORDER BY pub_date DESC LIMIT 1",
		response.site.siteId,
	).Scan(&latestPost)
	if err != nil {
		fmt.Println("Could not get most recent post for", response.site.feedName)
		return false
	}
	latestPostDate, err := time.Parse("2006-01-02 15:04:05", latestPost)
	if err != nil {
		fmt.Println("Could not parse datetime string", latestPost, response.site.feedName)
		return false
	}
	secondsSinceLastPost := time.Now().Unix() - latestPostDate.Unix()
	daysSinceLastPost := float64(secondsSinceLastPost) / float64(86400)
	stmt, err := db.Prepare("UPDATE sites SET days_since_last_post = ? WHERE pk_site_id = ?")
	if err != nil {
		fmt.Println(
			"Could not prepare SQL statement to update days_since_last_post", response.site.feedUrl, err.Error(),
		)
		return false
	}
	_, err = stmt.Exec(
		math.Round(daysSinceLastPost),
		response.site.siteId,
	)
	if err != nil {
		fmt.Println(
			"Could not execute SQL statement to update days_since_last_post", response.site.feedUrl, err.Error(),
		)
		return false
	}

	return true
}

func updateSitePosts(db *sql.DB, response SiteResponse) bool {
	for _, post := range response.site.posts {
		insertSitePost(db, post, response)
	}

	return true
}

func alreadyInsertdPost(db *sql.DB, post Post) int {
	var postsWithLink int
	err := db.QueryRow("SELECT COUNT(*) AS ttl FROM posts WHERE link = ?", post.link).Scan(&postsWithLink)
	if err != nil {
		fmt.Println("Could not count site posts", post.link, err.Error())
		return -1
	}
	return postsWithLink
}

func insertSitePost(db *sql.DB, post Post, response SiteResponse) {
	postsWithLink := alreadyInsertdPost(db, post)
	if postsWithLink == 0 {
		stmt, err := db.Prepare("INSERT INTO `posts` " +
			"(`fk_site_id`, `post_title`, `pub_date`, `link`, `description`, `content`) " +
			"VALUES (?, ?, ?, ?, ?, ?)")
		if err != nil {
			fmt.Println(
				"Could not prepare SQL statement to insert post",
				response.site.feedUrl,
				post.postTitle,
				err.Error(),
			)
			return
		}
		insertedPost, err := stmt.Exec(
			response.site.siteId,
			post.postTitle,
			post.pubDate,
			post.link,
			post.description,
			post.content,
		)
		if err != nil {
			fmt.Println(
				"Could not execute SQL statement to insert post",
				response.site.feedUrl,
				post.postTitle,
				err.Error(),
			)
			return
		}

		fmt.Println("Added [", response.site.feedName, "]", post.postTitle)

		postId, err := insertedPost.LastInsertId()
		if err != nil {
			fmt.Println(
				"Could not get post's LastInsertId()",
				response.site.feedUrl,
				post.postTitle,
				err.Error(),
			)
			return
		}

		insertPostTags(db, post, postId)
		insertMediaRelations(db, post, postId)
	}
	return
}

func insertPostTags(db *sql.DB, post Post, postId int64) {
	if len(post.categories) > 0 {
		for _, category := range post.categories {
			var tagId int64
			lowerTag := strings.TrimSpace(strings.ToLower(category))
			err := db.QueryRow("SELECT pk_tag_id FROM tags WHERE tag = ?", lowerTag).Scan(&tagId)
			if err != nil {
				if err.Error() != "sql: no rows in result set" {
					fmt.Println(
						"Could not search for tag",
						lowerTag,
						err.Error(),
					)
					return
				}
			}
			if tagId == 0 {
				stmt, err := db.Prepare("INSERT INTO `tags` " +
					"(`tag`) " +
					"VALUES (?)")
				if err != nil {
					fmt.Println(
						"Could not prepare SQL statement to insert tag",
						lowerTag,
						err.Error(),
					)
					return
				}
				inserted, err := stmt.Exec(
					lowerTag,
				)
				if err != nil {
					fmt.Println(
						"Could not execute SQL statement to insert tag",
						lowerTag,
						err.Error(),
					)
					return
				}
				tagId, err = inserted.LastInsertId()
				if err != nil {
					fmt.Println(
						"Could not get tag's LastInsertId()",
						lowerTag,
						err.Error(),
					)
					return
				}
			}
			stmt, err := db.Prepare("INSERT INTO `post_tags` " +
				"(`fk_post_id`, `fk_tag_id`) " +
				"VALUES (?, ?)")
			if err != nil {
				fmt.Println(
					"Could not prepare SQL statement to insert into post_tags",
					postId,
					tagId,
					err.Error(),
				)
				return
			}
			_, err = stmt.Exec(
				postId,
				tagId,
			)
			if err != nil {
				fmt.Println(
					"Could not execute SQL statement to insert into post_tags",
					postId,
					tagId,
					err.Error(),
				)
				return
			}
			fmt.Println("Inserted tag [", lowerTag, "] for post [", post.postTitle, "]")
		}
	}
}

func insertMediaRelations(db *sql.DB, post Post, postId int64) {
	for _, pkMediaId := range post.MediaIds {
		stmt, err := db.Prepare("INSERT INTO `post_media` " +
			"(`fk_post_id`, `fk_media_id`) " +
			"VALUES (?, ?)")
		if err != nil {
			fmt.Println(
				"Could not prepare SQL statement to insert post media",
				err.Error(),
			)
			return
		}
		_, err = stmt.Exec(
			postId,
			pkMediaId,
		)
		if err != nil {
			fmt.Println(
				"Could not execute SQL statement to insert post media",
				err.Error(),
			)
			return
		}
	}
}

func getCurrentDateTimeUtc() string {
	currentDateTimeUtc := time.Now().UTC().Format("2006-01-02 15:04:05")
	return currentDateTimeUtc
}

func updateSolr(solrBaseUrl string, httpClient *http.Client) {
	solrDeltaImportUrl := solrBaseUrl + "/dataimport?command=delta-import"

	req, err := http.NewRequest("GET", solrDeltaImportUrl, nil)
	if err != nil {
		fmt.Println("Could not create request for Solr delta import", err.Error())
		return
	}

	req.Header.Add("User-Agent", "@bateszi RSS feed parser")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)

	defer func(cancel context.CancelFunc) {
		cancel()
	}(cancel)

	req = req.WithContext(ctx)

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println("Error performing Solr delta import", err.Error())
		return
	}

	fmt.Println("Solr updated @", solrDeltaImportUrl)

	defer func(resp *http.Response) {
		_ = resp.Body.Close()
	}(resp)
}

func associateWithMedia(db *sql.DB, sites []SiteResponse) {
	var ttlMediaTitles float64
	err := db.QueryRow("SELECT COUNT(*) AS ttl FROM media_titles").Scan(&ttlMediaTitles)
	if err != nil && err.Error() != "sql: no rows in result set" {
		fmt.Println("could not count media_titles", err.Error())
		return
	}

	if ttlMediaTitles > 0 {
		mediaTitles := make([]MediaItem, 0)

		getRows, err := db.Query(
			"SELECT media_titles.fk_media_id, media_titles.title FROM media_titles, media WHERE fk_media_id = pk_media_id AND auto_index = 1 ORDER BY title ASC",
		)
		if err != nil {
			fmt.Println("Could not get media_titles", err.Error())
			return
		}

		defer func(getRows *sql.Rows) {
			err := getRows.Close()
			if err != nil {
				panic(err)
			}
		}(getRows)

		for getRows.Next() {
			var fkMediaId int64
			var title string

			err = getRows.Scan(
				&fkMediaId,
				&title,
			)
			if err != nil {
				fmt.Println("Could not read row of media_titles", err.Error())
				return
			}

			mediaTitles = append(mediaTitles, MediaItem{
				MediaID: fkMediaId,
				Title:   title,
			})
		}

		postsToCheck := make([]MediaMatched, 0)

		for siteKey, _ := range sites {
			siteResponse := sites[siteKey]
			if siteResponse.hasRssDoc {
				for postKey, post := range siteResponse.site.posts {
					if alreadyInsertdPost(db, post) == 0 {
						postsToCheck = append(postsToCheck, MediaMatched{
							SiteKey:    siteKey,
							PostKey:    postKey,
							Post:       post,
							MediaItems: make(map[int64]string),
						})
					}
				}
			}
		}

		ttlNewPosts := len(postsToCheck)

		if ttlNewPosts > 0 {
			mediaMatchedChan := make(chan MediaMatched, ttlNewPosts)

			for containerKey := range postsToCheck {
				associatingMediaWg.Add(1)
				fmt.Println("Attempting to match", postsToCheck[containerKey].Post.postTitle)

				go matchMediaToPost(mediaTitles, postsToCheck[containerKey], mediaMatchedChan)
			}

			fmt.Println("Waiting to finish matching posts to media...")
			associatingMediaWg.Wait()
			close(mediaMatchedChan)

			fmt.Println("New posts channel has", len(mediaMatchedChan), "items")

			for j := 0; j < ttlNewPosts; j++ {
				container := <-mediaMatchedChan

				if len(container.MediaItems) > 0 {
					for mediaId, _ := range container.MediaItems {
						sites[container.SiteKey].site.posts[container.PostKey].MediaIds = append(sites[container.SiteKey].site.posts[container.PostKey].MediaIds, mediaId)
					}
				}
			}
		}
	}
}

func matchMediaToPost(mediaTitles []MediaItem, container MediaMatched, mediaMatchedChan chan<- MediaMatched) {
	defer func(container MediaMatched) {
		fmt.Println("Finished searching", container.Post.link)
		associatingMediaWg.Done()
	}(container)

	postStrings := make([]string, 0)
	postStrings = append(postStrings, container.Post.postTitle, container.Post.description)

	for _, category := range container.Post.categories {
		postStrings = append(postStrings, category)
	}

	for _, mediaItem := range mediaTitles {
		for j := range postStrings {
			postString := postStrings[j]
			matched := strings.Contains(postString, mediaItem.Title)

			if matched {
				fmt.Println("Found match", mediaItem.Title, "for", container.Post.link)
				container.MediaItems[mediaItem.MediaID] = mediaItem.Title
			}
		}
	}

	mediaMatchedChan <- container
	fmt.Println("EOF", container.Post.link)
}

func start() {
	fmt.Println("Starting feed round at", time.Now().Format(time.RFC1123Z))
	encodedJson, err := ioutil.ReadFile("config/config.json")
	if err != nil {
		panic(err)
	}

	config := AppConfig{}

	err = json.Unmarshal(encodedJson, &config)
	if err != nil {
		panic(err)
	}

	dbParams := make(map[string]string)
	dbParams["charset"] = "utf8mb4"

	dbConfig := mysql.Config{
		User: config.Db.User,
		Passwd: config.Db.Password,
		Net: "tcp",
		Addr: config.Db.Server,
		DBName: config.Db.DbName,
		Params: dbParams,
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		panic(err)
	}

	defer func(db *sql.DB) {
		fmt.Println("Closing database connection at", time.Now().Format(time.RFC1123Z))
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from database server being down", r)
		}
	}()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("Opened database connection")

	sitesToFetch := getFeeds(db)
	ttlSitesToFetch := len(sitesToFetch)

	if ttlSitesToFetch > 0 {
		httpClient := &http.Client{}
		rssChannel := make(chan SiteResponse, ttlSitesToFetch)

		for _, siteToFetch := range sitesToFetch {
			fetchingFeedsWg.Add(1)

			go getSiteFeed(httpClient, siteToFetch, rssChannel)
		}

		fetchingFeedsWg.Wait()
		fmt.Println("Finished fetching feeds")
		close(rssChannel)

		responses := make([]SiteResponse, 0)

		for j := 0; j < ttlSitesToFetch; j++ {
			siteResponse := <-rssChannel
			responses = append(responses, siteResponse)
		}

		associateWithMedia(db, responses)

		for _, siteResponse := range responses {
			if siteResponse.hasRssDoc {
				updateSiteInfo(db, siteResponse)
				updateSitePosts(db, siteResponse)
				updateSiteDaysSinceStat(db, siteResponse)
			}
		}

		updateSolr(config.Solr, httpClient)
	}
}

func runFeedFetcher(d time.Duration) {
	ticker := time.NewTicker(d)

	for _ = range ticker.C {
		start()
	}
}

func main() {
	start()

	interval := 10 * time.Minute
	go runFeedFetcher(interval)

	fmt.Println("Starting ticker to fetch feeds every", interval)

	// Run application indefinitely
	select{}
}
