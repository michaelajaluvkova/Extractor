import json
import re
from random import uniform
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def random_delay(a=0.7, b=1.5):
    sleep(uniform(a, b))


class WebScrapper():
    def __init__(self):
        base_url = ''
        options = Options()
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        self.driver = webdriver.Chrome(options)
        self.driver.get(base_url)
        self.email = ''
        self.password = 'Uq&&'
        self.wait = WebDriverWait(self.driver, 15)
        self.all_scraped_data = []
        self.subcategories_base_url = ''
        self.visited_ids = set()
        self.json_data = {}
        self.slug_queue = []
        self.visited_urls = set()
        self.second_half_ids = []
        self.third_half_ids = []

    def sign_in(self):
        """
        Function opens the browser and log you in.
        :return:
        """
        WebDriverWait(self.driver, 15).until(EC.presence_of_element_located(
            (By.XPATH, "//a[@class='btn btn-primary btn-big col-12 border-box google-blue-bg mt2']")))
        random_delay()

        google_login_btn = self.driver.find_element(By.XPATH,
                                                    "//a[@class='btn btn-primary btn-big col-12 border-box google-blue-bg mt2']")
        ActionChains(self.driver).move_to_element(google_login_btn).click().perform()
        random_delay()

        self.wait.until(EC.presence_of_element_located((By.ID, 'identifierId')))
        try:
            email_box = self.driver.find_element(By.ID, 'identifierId')
        except NoSuchElementException:
            self.driver.refresh()
            email_box = self.driver.find_element(By.ID, 'identifierId')
        ActionChains(self.driver).move_to_element(email_box).click().send_keys(self.email).perform()
        random_delay()

        self.driver.find_element(By.ID, 'identifierNext').click()
        random_delay()

        WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id ="password"]/div[1]/div / div[1]/input')))
        random_delay()

        password_box = self.driver.find_element(By.XPATH, '//*[@id ="password"]/div[1]/div / div[1]/input')
        next_button = self.wait.until(EC.element_to_be_clickable((By.ID, 'passwordNext')))
        ActionChains(self.driver).move_to_element(password_box).click().send_keys(self.password).perform()
        random_delay()

        self.wait.until(EC.element_to_be_clickable((By.ID, 'passwordNext'))).click()
        random_delay()

        official_page_url = ''
        self.wait.until(EC.url_to_be(official_page_url))
        random_delay()

        return self.driver.current_url

    def extract_ordered_categories_ids(self, html_source):
        """
        :param html_source:
        :return:
        """
        ids = []
        soup = BeautifulSoup(html_source, 'html.parser')
        scripts = soup.find_all('script')
        for script in scripts:
            if 'orderedCategories' in script.string if script.string else '':
                match = re.search(r'var orderedCategories = (\[.*?\]);', script.string)
                if match:
                    ordered_categories_json = match.group(1)
                    ordered_categories = json.loads(ordered_categories_json)
                    ids = [category['id'] for category in ordered_categories]
        return ids if ids else []


    def check_for_404(self, page_source):
        if "Error - 404" in page_source:
            return True
        return False

    def introductory_scrapping(self):
        """
        Open the main page, takes first half of the category_ids and insert them into navigate_to_category func.
        :return:
        """
        current_url = self.sign_in()
        scraped_url = self.driver.current_url

        try:
            self.driver.get(scraped_url)
        except TimeoutException:
            print("Timed out. Skipping this one.")

        random_delay()
        html_source = self.driver.page_source
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        pretty_soup = soup.prettify()
        print(pretty_soup)
        category_ids = self.extract_ordered_categories_ids(html_source)
        print("Extracted Category IDs: ", category_ids)
        len_cat = len(category_ids)
        len_2 = len_cat // 2
        first_half_category_ids = category_ids[:len_2]
        self.second_Half_ids = category_ids[len_2:]
        for cat_id in first_half_category_ids:
            self.navigate_to_category(cat_id)

        self.process_slugs()

        return html_source

    base_url = ''

    def navigate_to_category(self, category_id):
        """
        Navigate you to each category page with category_id and scrape the subcategory ids.
        :param category_id:
        :return:
        """
        new_url = f"{self.base_url}{category_id}"
        if new_url in self.visited_urls:
            if new_url != '':
                print(f" Skipping already visited URL: {new_url}")
                return "skipped"
        else:
            print(f" Visiting new URL: {new_url}")
            self.visited_urls.add(new_url)
        try:
            self.driver.get(new_url)
        except TimeoutException:
            print("Timed out. Skipping this one.")
        random_delay()
        self.wait.until(EC.url_to_be(new_url))
        page_source = self.driver.page_source
        pinnable_ids = self.extract_pinnables_ids(page_source) if self.extract_pinnables_ids(page_source) else []
        subcategory_ids = self.recursive_scrape(page_source) if self.recursive_scrape(page_source) else []
        print("Extracted Pinnable IDs: ", pinnable_ids)
        pinnable_ids.extend(subcategory_ids)

        return list(pinnable_ids)

    def extract_pinnables_ids(self, page_source):
        """
        Exctract more ids for subcategory and insert to navigate_to_subcategory.
        :param page_source:
        :return:
        """
        ids = []
        match = re.search(r"var pinnables = (\{.*?\});", page_source)
        if match:
            pinnables = json.loads(match.group(1))
            ids = [data['id'] for data in pinnables['data']]
            for id in ids:
                self.navigate_to_subcategory(id)
            return ids
        return ids if ids else []

    def navigate_to_subcategory(self, subcategory_id):
        """
        Create pages with subcategory ids.
        :param subcategory_id:
        :return:
        """
        new_url = f"{self.subcategories_base_url}{subcategory_id}"
        if new_url in self.visited_urls:
            if new_url != '':
                print(f" Skipping already visited URL: {new_url}")
                return "skipped"
        else:
            print(f"Visiting new URL: {new_url}")
            self.visited_urls.add(new_url)
        try:
            self.driver.get(new_url)
        except TimeoutException:
            print("Timed out. Skipping this one.")
        random_delay()
        self.wait.until(EC.url_to_be(new_url))
        page_source = self.driver.page_source
        if self.check_for_404(page_source):
            print(f"404 Error for ID {subcategory_id}! Redirecting to category.")
            self.navigate_to_category(subcategory_id)
            return
        else:
            self.recursive_scrape(page_source)

    def scrape_page(self, page_source):
        """
        Does look for slugs (eg. names of articles behing backslash).
        :param page_source:
        :return:
        """
        slug_matches = re.findall(r'"slug":"(.*?)"', page_source)

        for slug in slug_matches:
            if slug != "none":
                url3 = f'https:/{slug}'
                if url3 in self.visited_urls:
                    continue
                else:
                    print(f"visiting new url from slug {url3}")
                    self.visited_urls.add(url3)
                try:
                    self.driver.get(url3)
                except TimeoutException:
                    print("Timed out. Skipping this one.")
                    continue
                random_delay()
                page_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                pretty_soup = page_soup.prettify()

                self.all_scraped_data.append({"url": url3, "content": pretty_soup})
            else:
                print("Skipping the 'none' slug!")

    def recursive_scrape(self, page_source):
        """
        This function is in charge of recursive scrapping, e.g. it inserts every possible id which could be found in html source, if it was already visited it skips the page.
        :param page_source:
        :return:
        """
        self.scrape_page(page_source)
        ids = []
        subcategory_match = re.search(r'var subcategory = (\{.*?\});', page_source)
        if subcategory_match:
            subcategory_data = json.loads(subcategory_match.group(1))
            print(type(subcategory_data))
            print(subcategory_data)

            if type(subcategory_data) is list:
                ids = [item['id'] for item in subcategory_data if 'id' in item and item['id'] != 'none']
            elif type(subcategory_data) is dict:
                ids = [item['id'] for item in subcategory_data.values() if
                       isinstance(item, dict) and 'id' in item and item['id'] != 'none']
            else:
                print("Unexpected data type")

        folder_items_match = re.search(r'var folderItemsMap = ({.*?});', page_source)
        if folder_items_match:
            folder_items_map_str = folder_items_match.group(1)
            folder_items_map = json.loads(folder_items_map_str)

            for folder_id, folder_items in folder_items_map.items():
                for page in folder_items.get("pages", []):
                    ids.append(page["id"])
                    ids.append(page["folder_id"])
                    ids.append(page["category_id"])
                    self.slug_queue.append(page["slug"])

        blabla = re.search(r'var subcategory = ({.*?});', page_source)
        if blabla:
            folder_items_str = blabla.group(1)
            blabla_items = json.loads(folder_items_str)

            for item in blabla_items:
                if blabla_items.get("id"):
                    ids.append(blabla_items["id"])
                if blabla_items.get("category_id"):
                    ids.append(blabla_items["category_id"])
                if blabla_items.get("slug") != "none":
                    self.slug_queue.append(blabla_items["slug"])

        folder_items_match1 = re.search(r'var folderItems = (\[.*?\]);', page_source)
        if folder_items_match1:
            folder_items_str = folder_items_match1.group(1)
            folder_items = json.loads(folder_items_str)

            for item in folder_items:
                ids.append(item["id"])
                ids.append(item["folder_id"])
                ids.append(item["category_id"])
                if item["slug"] != "none":
                    self.slug_queue.append(item["slug"])

        for id in ids:
            self.navigate_to_subcategory(id)

        return ids if ids else []

    def process_slugs(self):
        """
        Does scrape specific page of company articles.
        :return:
        """
        print(f" Starting with {len(self.slug_queue)} slugs in the queue.")
        for slug in self.slug_queue:
            url3 = f'https:///{slug}'
            if url3 in self.visited_urls:
                continue
            else:
                print(f"visiting new url {url3}")
                self.visited_urls.add(url3)
            try:
                self.driver.get(url3)
            except TimeoutException:
                print("Timed out. Skipping this one.")
                continue
            random_delay()

            page_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            pretty_soup = page_soup.prettify()

            self.all_scraped_data.append({"url": url3, "content": pretty_soup})
            self.slug_queue.remove(slug)

    def save_to_json(self):
        """
        Saves final scrape into json file.
        :return:
        """
        print(self.all_scraped_data)
        with open("all_scraped_data.json", "w") as f:
            json.dump(self.all_scraped_data, f)

    def introductory_scrapping_2(self):
        """
        Does the same as introductory_scraping but for second part of category_ids.
        :return:
        """
        current_url = self.sign_in()
        scraped_url = self.driver.current_url
        try:
            self.driver.get(scraped_url)
        except TimeoutException:
            print("Timed out. Skipping this one.")
        random_delay()
        html_source = self.driver.page_source
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        pretty_soup = soup.prettify()
        print(pretty_soup)
        category_ids = self.extract_ordered_categories_ids(html_source)
        print("Extracted Category IDs: ", category_ids)
        len_cat = len(category_ids)
        len_half = len_cat // 2
        len_quarter = len_half // 2
        self.second_half_ids = category_ids[len_half:len_half + len_quarter]

        print(self.second_half_ids)
        for cat_id in self.second_half_ids:
            self.navigate_to_category(cat_id)

        self.process_slugs()

        return html_source

    def save_to_json_2(self):
        """
        Does the same as save_to_json but for the second half of category ids.
        :return:
        """
        print(self.all_scraped_data)
        with open("all_scraped_data_2.json", "w") as f:
            json.dump(self.all_scraped_data, f)
    def introductory_scrapping_3(self):
        """
        Does the same as introductory_scraping but for third part of category_ids.
        :return:
        """
        current_url = self.sign_in()
        scraped_url = self.driver.current_url
        try:
            self.driver.get(scraped_url)
        except TimeoutException:
            print("Timed out. Skipping this one.")
        random_delay()
        html_source = self.driver.page_source
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        pretty_soup = soup.prettify()
        print(pretty_soup)
        category_ids = self.extract_ordered_categories_ids(html_source)
        print("Extracted Category IDs: ", category_ids)
        len_cat = len(category_ids)
        len_half = len_cat // 2
        len_quarter = len_half // 2

        self.third_half_ids = category_ids[len_half + len_quarter:]

        print(self.third_half_ids)
        for cat_id in self.third_half_ids:
            self.navigate_to_category(cat_id)

        self.process_slugs()

        return html_source

    def save_to_json_3(self):
        """
        Does the same as save_to_json but for the third half of category ids.
        :return:
        """
        print(self.all_scraped_data)
        with open("all_scraped_data_3.json", "w") as f:
            json.dump(self.all_scraped_data, f)

### Turns off the print statements ###
original_print = print
print = lambda *args, **kwargs: None

client = WebScrapper()
client.introductory_scrapping()
client.save_to_json()
client.driver.quit()
client = WebScrapper()
client.introductory_scrapping_2()
client.save_to_json_2()
client.driver.quit()
client = WebScrapper()
client.introductory_scrapping_3()
client.save_to_json_3()
client.driver.quit()


with open('all_scraped_data.json', 'r') as file:
    data_1 = json.load(file)
with open('all_scraped_data_2.json', 'r') as file:
    data_2 = json.load(file)
with open('all_scraped_data_3.json', 'r') as file:
    data_3 = json.load(file)

merged_data = data_1 + data_2 + data_3

with open('all_scrapped_data_combined.json', 'w') as file:
    json.dump(merged_data, file, indent=4)

#print("Merging completed. Everything completed.")

original_print("Merging completed. Everything completed.") ### do not open until turning off print statements
