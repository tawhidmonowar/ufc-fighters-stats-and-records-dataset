## Fresh Install Scrapy

### 1. Create a Virtual Environment in Python

First, create a virtual environment to isolate your project dependencies:

```python -m venv .venv```

### 2. Install Scrapy

Next, install Scrapy inside the virtual environment:

```pip install scrapy```

### 3. Create a New Scrapy Project

Create a new Scrapy project by running the following command. This will generate the necessary directory structure and files:

```scrapy startproject ufc_scraper```

### 4. Move to the Project Folder
Navigate to the newly created project directory:

```cd ufc_scraper```

### 5. Create a Spider
Create a new spider to scrape data from a website. In this case, we are using "ufc.com":

```scrapy genspider ufc_spider ufc.com```

### 6. To Run the Spider
Finally, run your spider to start scraping:

```scrapy crawl ufc_spider```


## Clone From GitHub

To clone an existing Scrapy project from GitHub:

### 1. Copy the repository URL.
Run the following command:

```git clone <repository_url>```

### 2. Install dependencies:

```pip install -r requirements.txt```

### 3. Run the spider as usual:

```scrapy crawl <spider_name>```
