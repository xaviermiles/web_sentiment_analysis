# Scraping and Crawling

## Crawling - Inital Outlay
There is a wide variety of web-crawls/crawlers, but they can be grouped into two broad types:
- broad crawls - wanting crawlers to gather information from a (large) group of websites with the crawler *naturally* traversing the internet, which allows for switching between websites if they are linked together (domain-agnostic).
- focussed crawls - wanting crawlers to gather specific information and these will tend to be constrained to given domains (or even URLs).

This project will perform focussed crawls from a specified list of websites to gather specific information that pertains to whether these websites can be linked to a New Zealand business, have any shopping cart software, etc. However, this will be on a LARGE number of websites so the crawlers will have to be fairly general in the way that they approach websites (e.g. will have to be intelligent when looking for an "About Us" section, since websites will have different subdomain-URL structures).

There is four main considerations when setting up (or maintaining) web-crawlers:
- selection policy - which pages to download. We will use a list of websites to start the crawlers, but this crawling will involve searching for links on the main page of the websites, and following these links to other parts of the website (subdomain). Since not every page on a website will contain useful information, selection criterion should be used to tell/inform the crawlers which pages on a website are worth downloading and processing. The traditional way to do this is using importance metrics (e.g. PageRank, On-line Page Importance Computation), which use the number of inbound/outbound links to/from other webpages. For this project, it may be appropriate to explicitly state which types of webpages we are looking for, such as "About Us" or "Contact Us" pages. Both approaches could be used together.
- revisit policy - how often to revisit websites to collect information. This might be set by others in Stats NZ to guarantee consistency in data collection (eg. quarterly or monthly), but this will also be related to computational resources. Subsequent crawls may not need to revisit as much of websites if nothing has changed. For example, changes to a website, and subsequently whether their link to a NZ business and/or digital industry classification may have changed, can be detected by consulting a website's sitemap if the sitemap is in xml format (which is typical) and includes "last modified" tags (e.g. www.macpac.co.nz/sitemap_0.xml).
- politeness policy - how to avoid overloading websites. This is a purely technical decision. This is usually informed by consulting the robots policy on website (e.g. www.macpac.co.nz/robots.txt), but could also be made based on the apparent speed of the website's server.
- parallelisation policy - how to coordinate distributed web crawlers. This will be informed by the crawling framework/technology used.

Very good resource for general info on web crawlers: [not-wikipedia](https://en.wikipedia.org/wiki/Web_crawler).

## Software to use

*FILL OUT MORE*

Scrapy (Python), Apache Nutch (Java), rvest (R)

https://michaelnielsen.org/ddi/how-to-crawl-a-quarter-billion-webpages-in-40-hours/
