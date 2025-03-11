import os
import json
import scrapy

class UfcSpider(scrapy.Spider):
    name = "ufc_spider"
    allowed_domains = ["ufc.com"]
    start_urls = ["https://www.ufc.com/athletes/all?gender=1"]  # Male fighters

    custom_settings = {
        "ROBOTSTXT_OBEY": False
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fighter_data = []  # Store all fighter data
        self.page_count = 1  # Track pagination
        self.fighter_count = 0  # Track fighters scraped

    def parse(self, response, **kwargs):
        print(f"Scraping page {self.page_count}: {response.url}")
        athletes = response.css(".c-listing-athlete-flipcard")
        for index, athlete in enumerate(athletes, start=1):
            profile_link = response.urljoin(athlete.css(".e-button--black::attr(href)").get(default="").strip())
            print(f"Scraping fighter {self.fighter_count + index} from page {self.page_count}: {profile_link}")
            yield response.follow(profile_link, self.parse_profile)

        self.fighter_count += len(athletes)

        # Handle pagination
        next_page = response.css(".pager__item.pager__item--next a::attr(href)").get()
        if next_page:
            self.page_count += 1
            yield response.follow(response.urljoin(next_page), self.parse)

    def parse_profile(self, response):
        def clean_text(row_text):
            return row_text.replace('"', '').strip() if row_text else ""

        # Extracts fighter details
        about = {
            "id": response.url.split("/")[-1].split("?")[0],
            "name": response.css(".hero-profile .hero-profile__name::text").get(default="").strip(),
            "nickname": clean_text(response.css(".hero-profile .hero-profile__nickname::text").get(default="").strip()),
            "division": response.css(".hero-profile .hero-profile__division-title::text").get(default="").strip(),
            "gender": "Male"
        }
        about_details = response.css('div.c-bio__info-details')
        if about_details:
            for field in about_details.css('div.c-bio__field'):
                label = field.css('div.c-bio__label::text').get()
                text = field.css('div.c-bio__text::text').get()
                if label and text:
                    if label == "Age":
                        age_text = field.css('div.field__item::text').get()
                        about[label.strip()] = age_text.strip()
                    else:
                        about[label.strip()] = text.strip()

        stats = {}
        stats_details = response.css('.l-container__content')
        if stats_details:
            for field in stats_details.css(".c-overlap__stats"):
                label = field.css(".c-overlap__stats-text::text").get()
                text = field.css(".c-overlap__stats-value::text").get()
                if label and text:
                    stats[label.strip()] = text.strip()
                else:
                    stats[label.strip()] = '0'

            for field in stats_details.css(".c-stat-compare__group"):
                label_suffix = field.css(".c-stat-compare__label-suffix::text").get("")
                label_prefix = field.css(".c-stat-compare__label::text").get("")
                label = (label_prefix + " " + label_suffix)
                text_suffix = field.css(".c-stat-compare__percent::text").get("")
                text_prefix = field.css(".c-stat-compare__number::text").get("")
                text = (text_prefix.strip() + text_suffix.strip()) if text_prefix and text_suffix else "0"
                if label and text:
                    stats[label.strip()] = text.strip()
                else:
                    stats[label.strip()] = '0'

            for field in stats_details.css(".c-stat-3bar__group"):
                label = field.css(".c-stat-3bar__label::text").get()
                text = field.css(".c-stat-3bar__value::text").get()
                if label and text:
                    stats[label.strip()] = text.strip()
                else:
                    stats[label.strip()] = '0'

            svg_container = stats_details.css('.c-stat-body__diagram')
            if svg_container:
                svg_groups = {
                    'Head': 'e-stat-body_x5F__x5F_head-txt',
                    'Body': 'e-stat-body_x5F__x5F_body-txt',
                    'Leg': 'e-stat-body_x5F__x5F_leg-txt'
                }
                for label, group_id in svg_groups.items():
                    group = svg_container.xpath(f'.//g[@id="{group_id}"]')
                    if group:
                        value = group.xpath('.//text[@fill="#D20A0A"][2]/text()').get()
                        stats[label] = value.strip() if value else '0'

        record = {
            "wld": response.css(".hero-profile .hero-profile__division-body::text").get(default="").strip(),
        }

        athlete_record = response.css('div.athlete-stats')
        if athlete_record:
            for stat_div in athlete_record.css('div.athlete-stats__stat'):
                number = stat_div.css('p.athlete-stats__stat-numb::text').get()
                text = stat_div.css('p.athlete-stats__stat-text::text').get()
                if number and text:
                    record[text.strip()] = number.strip()

        fight_history = response.meta.get('fight_history', {})
        fight_record = response.css('article.c-card-event--athlete-results')
        for fight in fight_record:

            fighter_urls = fight.css('h3.c-card-event--athlete-results__headline a::attr(href)').getall()
            fighter1_id = fighter_urls[0].split('/')[-1] if fighter_urls else "unknown"
            fighter2_id = fighter_urls[1].split('/')[-1] if len(fighter_urls) > 1 else "unknown"

            winner_div = fight.css('div.c-card-event--athlete-results__plaque.win')
            if winner_div:
                winner_url = fight.css('div.c-card-event--athlete-results__blue-image a::attr(href)').get()
                winner_id = winner_url.split('/')[-1] if winner_url else fighter2_id
                loser_id = fighter1_id if winner_id == fighter2_id else fighter2_id
            else:
                winner_id = "unknown"
                loser_id = "unknown"

                # Extract fight details
                date = fight.css('div.c-card-event--athlete-results__date::text').get(default="N/A").strip()
                results = fight.css('div.c-card-event--athlete-results__results')
                round = results.css('div.c-card-event--athlete-results__result:contains("Round") div.c-card-event--athlete-results__result-text::text').get(default="N/A")
                time = results.css('div.c-card-event--athlete-results__result:contains("Time") div.c-card-event--athlete-results__result-text::text').get(default="N/A")
                method = results.css('div.c-card-event--athlete-results__result:contains("Method") div.c-card-event--athlete-results__result-text::text').get( default="N/A")

                # Use fighter IDs in the fight_key
                fight_key = f"{fighter1_id}_vs_{fighter2_id}_{date}"
                fight_history[fight_key] = {
                    "fighter1_id": fighter1_id,
                    "fighter2_id": fighter2_id,
                    "winner_id": winner_id,
                    "loser_id": loser_id,
                    "date": date,
                    "round": round,
                    "time": time,
                    "method": method
                }

        # Check for pagination
        next_page = response.css('ul.js-pager__items.pager a::attr(href)').get()
        if next_page:
            # Pass the current data through meta to the next page
            meta = {
                'about': about,
                'stats': stats,
                'record': record,
                'fight_history': fight_history
            }
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_url, callback=self.parse_profile, meta=meta)
        else:
            # If no more pages, compile the final athlete data
            athlete_data = {
                "about": about,
                "stats": stats,
                "record": record,
                "fight_history": fight_history
            }
            self.fighter_data.append(athlete_data)
            yield athlete_data

    def closed(self, reason):
        self.save_data()
        print(f"Scraping complete. Total fighters scraped: {len(self.fighter_data)}")

    def save_data(self):
        file_path = "ufc_fighters_stats_and_records.json"
        existing_data = []

        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []

        existing_data.extend(self.fighter_data)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=4)
        print(f"Data appended to {file_path}")