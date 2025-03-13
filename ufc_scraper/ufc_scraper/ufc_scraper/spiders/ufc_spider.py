import os
import json
import scrapy

class UfcSpider(scrapy.Spider):
    name = "ufc_spider"
    allowed_domains = ["ufc.com"]
    start_urls = ["https://www.ufc.com/athletes/all?gender=1"]  # Male fighters
    output_file = "ufc_fighters_stats_and_records.json"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 1,
        "RETRY_TIMES": 3
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fighter_data = []
        self.page_count = 1
        self.fighter_count = 0
        # For tracking fight history pagination
        self.fighter_history_queue = {}  # Stores fighter profiles that need additional fight history pages

    def parse(self, response, **kwargs):
        """Main parsing function for athlete listing pages"""
        self.logger.info(f"Scraping page {self.page_count}: {response.url}")

        # Find all athlete cards on the page
        athletes = response.css(".c-listing-athlete-flipcard")
        self.logger.info(f"Found {len(athletes)} athletes on page {self.page_count}")

        # Process each athlete
        for index, athlete in enumerate(athletes, start=1):
            profile_link = self._extract_profile_link(response, athlete)
            if profile_link:
                self.logger.info(
                    f"Scraping fighter {self.fighter_count + index} from page {self.page_count}: {profile_link}")
                yield response.follow(profile_link, self.parse_profile)
            else:
                self.logger.warning(f"Could not extract profile link for athlete {self.fighter_count + index}")

        self.fighter_count += len(athletes)

        # Handle pagination
        next_page = self._get_next_page(response)
        if next_page:
            self.page_count += 1
            yield response.follow(next_page, self.parse)
        else:
            self.logger.info("No more pages found. Finishing scraping.")

    def _extract_profile_link(self, response, athlete):
        """Extract the profile link from an athlete card"""
        link = athlete.css(".e-button--black::attr(href)").get(default="").strip()
        return response.urljoin(link) if link else None

    def _get_next_page(self, response):
        """Extract the next page URL"""
        # Fixed the selector to correctly find the next page link
        # next_page = response.css(".dummy a::attr(href)").get()
        next_page = response.css(".pager__item a::attr(href)").get()
        return response.urljoin(next_page) if next_page else None

    def parse_profile(self, response):
        """Parse an individual fighter's profile page"""
        try:
            fighter_id = response.url.split("/")[-1].split("?")[0]

            about = self._extract_about_info(response)
            stats = self._extract_stats(response)
            record = self._extract_record(response)

            # Initial fight history extraction
            fight_history = self._extract_fight_history(response)

            athlete_data = {
                "about": about,
                "stats": stats,
                "record": record,
                "fight_history": fight_history
            }

            # Check if there's a "Load More" button for fight history
            load_more = response.css('.pager__item a::attr(href)').get()

            if load_more:
                # Store fighter data temporarily and follow the load more link
                self.fighter_history_queue[fighter_id] = {
                    'base_data': athlete_data,
                    'page': 1  # Start with page 1 for the next request
                }

                # Build the next page URL - handling both relative and absolute paths
                if load_more.startswith('http'):
                    next_url = load_more
                elif '?' in load_more:
                    # If URL already has parameters
                    next_url = response.url.split('?')[0] + load_more
                else:
                    # If URL doesn't have parameters
                    next_url = response.url + load_more

                self.logger.info(
                    f"Following fight history pagination for {about.get('name', 'Unknown Fighter')}: {next_url}")
                yield response.follow(
                    next_url,
                    callback=self.parse_fight_history_page,
                    meta={'fighter_id': fighter_id}
                )
            else:
                # No more fight history pages, add fighter data to the collection
                self.fighter_data.append(athlete_data)
                self.logger.info(f"Successfully scraped profile for {about.get('name', 'Unknown Fighter')}")

        except Exception as e:
            self.logger.error(f"Error parsing profile {response.url}: {str(e)}")

    def parse_fight_history_page(self, response):
        """Parse additional fight history pages"""
        fighter_id = response.meta.get('fighter_id')

        if not fighter_id or fighter_id not in self.fighter_history_queue:
            self.logger.error(f"Fighter ID missing or not in queue: {fighter_id}")
            return

        # Get the base fighter data
        fighter_data = self.fighter_history_queue[fighter_id]['base_data']
        current_page = self.fighter_history_queue[fighter_id]['page']

        # Extract additional fights from this page
        new_fights = self._extract_fight_history(response)

        # Add new fights to the existing fight history
        fighter_data['fight_history'].update(new_fights)

        # Check if there's another "Load More" button - look for any load more link
        load_more = response.css('.js-pager__items.pager a::attr(href)').get()

        if load_more:
            # There's another page to load
            next_page = current_page + 1
            self.fighter_history_queue[fighter_id]['page'] = next_page

            # Build the next URL
            if load_more.startswith('http'):
                next_url = load_more
            else:
                # Handle relative URLs
                base_url = response.url.split('?')[0]
                if load_more.startswith('?'):
                    next_url = f"{base_url}{load_more}"
                else:
                    next_url = f"{base_url}/{load_more.lstrip('/')}"

            self.logger.info(
                f"Following fight history pagination (page {next_page}) for fighter {fighter_id}: {next_url}")
            yield response.follow(
                next_url,
                callback=self.parse_fight_history_page,
                meta={'fighter_id': fighter_id}
            )
        else:
            # No more pages, add the complete fighter data to our collection
            self.fighter_data.append(fighter_data)
            self.logger.info(
                f"Completed fight history pagination for fighter {fighter_id}. Total fights: {len(fighter_data['fight_history'])}")

            # Remove fighter from the queue
            del self.fighter_history_queue[fighter_id]

    def _extract_about_info(self, response):
        """Extract basic information about a fighter"""
        about = {
            "id": response.url.split("/")[-1].split("?")[0],
            "name": response.css(".hero-profile .hero-profile__name::text").get(default="").strip(),
            "nickname": self._clean_text(response.css(".hero-profile .hero-profile__nickname::text").get(default="")),
            "division": response.css(".hero-profile .hero-profile__division-title::text").get(default="").strip(),
            "gender": "Male"
        }

        about_details = response.css('div.c-bio__info-details')
        if about_details:
            for field in about_details.css('div.c-bio__field'):
                self._process_bio_field(field, about)

        return about

    def _process_bio_field(self, field, about_dict):
        """Process a biographical field"""
        label = field.css('div.c-bio__label::text').get()
        if not label:
            return

        label = label.strip()

        if label == "Age":
            age_text = field.css('div.field__item::text').get()
            about_dict[label] = age_text.strip() if age_text else ""
        else:
            value = field.css('div.c-bio__text::text').get()
            about_dict[label] = value.strip() if value else ""

    def _extract_stats(self, response):
        """Extract fighter statistics"""
        stats = {}
        stats_details = response.css('.l-container__content')
        if not stats_details:
            return stats

        # Process overlap stats
        self._extract_overlap_stats(stats_details, stats)

        # Process comparative stats
        self._extract_comparative_stats(stats_details, stats)

        # Process 3-bar stats
        self._extract_3bar_stats(stats_details, stats)

        # Process body diagram stats
        self._extract_body_diagram_stats(stats_details, stats)

        return stats

    def _extract_overlap_stats(self, container, stats_dict):
        """Extract stats from c-overlap__stats sections"""
        for field in container.css(".c-overlap__stats"):
            label = field.css(".c-overlap__stats-text::text").get()
            text = field.css(".c-overlap__stats-value::text").get()
            if label:
                stats_dict[label.strip()] = text.strip() if text else '0'

    def _extract_comparative_stats(self, container, stats_dict):
        """Extract stats from c-stat-compare__group sections"""
        for field in container.css(".c-stat-compare__group"):
            label_suffix = field.css(".c-stat-compare__label-suffix::text").get("").strip()
            label_prefix = field.css(".c-stat-compare__label::text").get("").strip()
            label = (label_prefix + " " + label_suffix).strip()

            text_suffix = field.css(".c-stat-compare__percent::text").get("").strip()
            text_prefix = field.css(".c-stat-compare__number::text").get("").strip()
            text = (text_prefix + text_suffix) if (text_prefix or text_suffix) else "0"

            if label:
                stats_dict[label] = text

    def _extract_3bar_stats(self, container, stats_dict):
        """Extract stats from c-stat-3bar__group sections"""
        for field in container.css(".c-stat-3bar__group"):
            label = field.css(".c-stat-3bar__label::text").get()
            text = field.css(".c-stat-3bar__value::text").get()
            if label:
                stats_dict[label.strip()] = text.strip() if text else '0'

    def _extract_body_diagram_stats(self, container, stats_dict):
        """Extract stats from the body diagram SVG"""
        svg_container = container.css('.c-stat-body__diagram')
        if not svg_container:
            return

        svg_groups = {
            'Head': 'e-stat-body_x5F__x5F_head-txt',
            'Body': 'e-stat-body_x5F__x5F_body-txt',
            'Leg': 'e-stat-body_x5F__x5F_leg-txt'
        }

        for label, group_id in svg_groups.items():
            group = svg_container.xpath(f'.//g[@id="{group_id}"]')
            if group:
                value = group.xpath('.//text[@fill="#D20A0A"][2]/text()').get()
                stats_dict[label] = value.strip() if value else '0'

    def _extract_record(self, response):
        """Extract fighter record information"""
        record = {
            "wld": response.css(".hero-profile .hero-profile__division-body::text").get(default="").strip(),
        }

        athlete_record = response.css('div.athlete-stats')
        if athlete_record:
            for stat_div in athlete_record.css('div.athlete-stats__stat'):
                number = stat_div.css('p.athlete-stats__stat-numb::text').get()
                text = stat_div.css('p.athlete-stats__stat-text::text').get()
                if text:
                    record[text.strip()] = number.strip() if number else '0'

        return record

    def _format_date(self, date_string):
        # Format date string from "Mar. 19, 2022" to "Mar_19_2022"
        if not date_string:
            return None
        # Remove any periods
        date_string = date_string.replace(".", "")
        # Replace spaces and commas with underscores
        date_string = date_string.replace(" ", "_").replace(",", "")

        return date_string

    def _extract_fight_history(self, response):
        """Extract fighter's fight history"""
        fight_history = {}
        fight_records = response.css('article.c-card-event--athlete-results')

        for fight in fight_records:
            try:
                fight_data = self._process_fight_record(fight)
                if fight_data:
                    formatted_date = self._format_date(fight_data['date'])
                    fight_key = f"{fight_data['fighter1_id']}_vs_{fight_data['fighter2_id']}_{formatted_date}"
                    fight_history[fight_key] = fight_data
            except Exception as e:
                self.logger.error(f"Error processing fight record: {str(e)}")

        return fight_history

    def _process_fight_record(self, fight):
        """Process a single fight record"""
        # Get fighter names for reference
        fighter_names = fight.css('h3.c-card-event--athlete-results__headline a::text').getall()
        if not fighter_names:
            return None

        # Get fighter profile URLs to extract IDs
        fighter_urls = fight.css('h3.c-card-event--athlete-results__headline a::attr(href)').getall()

        # Extract fighter IDs from URLs
        fighter1_id = self._extract_fighter_id(fighter_urls[0]) if fighter_urls and len(fighter_urls) > 0 else "unknown"
        fighter2_id = self._extract_fighter_id(fighter_urls[1]) if fighter_urls and len(fighter_urls) > 1 else "unknown"

        # Keep fighter names for reference
        fighter1_name = fighter_names[0] if fighter_names else "Unknown"
        fighter2_name = fighter_names[1] if len(fighter_names) > 1 else "Unknown"

        # Determine winner and loser
        fighter1_div = fight.css('.c-card-event--athlete-results__red-image')
        fighter2_div = fight.css('.c-card-event--athlete-results__blue-image')

        # Check which fighter has the win plaque
        if fighter1_div.css('.c-card-event--athlete-results__plaque.win'):
            winner_id = fighter1_id
            loser_id = fighter2_id
            winner_name = fighter1_name
            loser_name = fighter2_name
        elif fighter2_div.css('.c-card-event--athlete-results__plaque.win'):
            winner_id = fighter2_id
            loser_id = fighter1_id
            winner_name = fighter2_name
            loser_name = fighter1_name
        else:
            # No winner found, might be a draw or no contest
            winner_id = "draw-no-contest"
            loser_id = "draw-no-contest"
            winner_name = "Draw/No Contest"
            loser_name = "Draw/No Contest"

        # Extract fight details
        date = fight.css('div.c-card-event--athlete-results__date::text').get(default="N/A").strip()
        results = fight.css('div.c-card-event--athlete-results__results')

        # Extract round, time, and method with more precise selectors
        round_num = "N/A"
        time = "N/A"
        method = "N/A"

        for result_div in results.css('div.c-card-event--athlete-results__result'):
            label = result_div.css('div.c-card-event--athlete-results__result-label::text').get("")
            value = result_div.css('div.c-card-event--athlete-results__result-text::text').get("")

            if label and "Round" in label:
                round_num = value.strip()
            elif label and "Time" in label:
                time = value.strip()
            elif label and "Method" in label:
                method = value.strip()

        # Get the event details from the fight card link
        event_link = fight.css('a[href*="event"]::attr(href)').get()
        event_name = "Unknown Event"
        event_id = "unknown-event"
        if event_link:
            # Extract event name and ID from URL
            event_parts = event_link.split('/')
            if len(event_parts) > 2:
                event_id = event_parts[-1].split('#')[0]
                event_name = event_id.replace('-', ' ').title()

        return {
            "fighter1": fighter1_name,
            "fighter2": fighter2_name,
            "fighter1_id": fighter1_id,
            "fighter2_id": fighter2_id,
            "winner": winner_name,
            "loser": loser_name,
            "winner_id": winner_id,
            "loser_id": loser_id,
            "date": date,
            "round": round_num,
            "time": time,
            "method": method,
            "event": event_name,
            "event_id": event_id
        }

    def _extract_fighter_id(self, url):
        """Extract fighter ID from profile URL"""
        if not url:
            return "unknown"

        parts = url.split('/')
        if len(parts) > 0:
            return parts[-1]

        return "unknown"

    def _clean_text(self, text):
        """Clean a text string"""
        return text.replace('"', '').strip() if text else ""

    def closed(self, reason):
        """Handle spider closing"""
        # Check if there are any fighters still in the pagination queue
        if self.fighter_history_queue:
            self.logger.warning(
                f"{len(self.fighter_history_queue)} fighters still in pagination queue when spider closed")
            # Add the incomplete data anyway
            for fighter_id, fighter_info in self.fighter_history_queue.items():
                self.fighter_data.append(fighter_info['base_data'])

        self.save_data()
        self.logger.info(f"Scraping complete. Total fighters scraped: {len(self.fighter_data)}")

    def save_data(self):
        """Save scraped data to JSON file"""
        existing_data = self._load_existing_data()

        # Merge existing data with new data
        merged_data = self._merge_data(existing_data)

        # Write merged data to file
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Data saved to {self.output_file}")

    def _load_existing_data(self):
        """Load existing data from JSON file if it exists"""
        if not os.path.exists(self.output_file):
            return []

        try:
            with open(self.output_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            self.logger.warning(f"Failed to load existing data from {self.output_file}, starting fresh")
            return []

    def _merge_data(self, existing_data):
        """Merge existing data with new data, avoiding duplicates"""
        # Create a dictionary of existing fighters by ID for quick lookup
        existing_ids = set()
        for fighter in existing_data:
            if 'about' in fighter and 'id' in fighter['about']:
                existing_ids.add(fighter['about']['id'])

        # Add only new fighters to the existing data
        for fighter in self.fighter_data:
            if 'about' in fighter and 'id' in fighter['about']:
                if fighter['about']['id'] not in existing_ids:
                    existing_data.append(fighter)
                    existing_ids.add(fighter['about']['id'])
            else:
                existing_data.append(fighter)

        return existing_data