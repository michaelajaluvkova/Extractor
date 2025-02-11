from bs4 import BeautifulSoup, NavigableString, Tag
import re

space_key = ""

def extract_owned_by(json_data):
    owned_by_names = []

    for item in json_data:
        content = item.get('content', '')
        pattern = r'<img\s+alt\s*=\s*"Owned by ([^"]+)"'
        matches = re.findall(pattern, content)
        owned_by_names.extend(matches)

    return owned_by_names

def extract_url(json_data, output_file):
    urls = []

    def extract(data):
        if isinstance(data, dict):
            for key, value in data.items():
                extract(value)
        elif isinstance(data, list):
            for item in data:
                extract(item)
        elif isinstance(data, str):
            url_pattern = re.compile(r'https?://\S+')
            urls.extend(url_pattern.findall(data))

    extract(json_data)

    with open(output_file, 'w') as file:
        for url in urls:
            #print(url)
            file.write(url + '\n')
    return urls

def encode_url_for_confluence(url):
    url = url.replace('&', '&amp;')
    confluence_url = f'<ac:link><ri:url ri:value="{url}"/></ac:link>'

    return confluence_url

def convert_html_to_confluence_format(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Clean up scripts, styles, and unnecessary tags
    for element in soup(["script", "style", "link", "br"]):
        element.decompose()

    # Convert divs to paragraphs
    for div in soup.find_all('div'):
        div.name = 'p'

    for ac_link in soup.find_all('ac:link'):
        # Find ri:url tag within ac:link
        ri_url_tag = ac_link.find('ri:url')
        if ri_url_tag:
            # Extract ri:value attribute (the actual URL)
            href = ri_url_tag.get('ri:value')

            # Create a new <a> tag with the href attribute
            new_a_tag = soup.new_tag('a', href=href)
            new_a_tag.string = ac_link.get_text(strip=True)  # Copy the text content inside ac:link

            # Replace ac:link with the new <a> tag
            ac_link.replace_with(new_a_tag)

    return str(soup)


confluence_API = ""
import json
import requests
from bs4 import BeautifulSoup

with open('', 'r') as file:
    data = json.load(file)

# Filters out the non existing or archived pages
def check_page_status(pages):
    total_pages = len(pages)
    accessible_pages = []

    for page in pages:
        url = page['url']
        content = page['content']
        soup = BeautifulSoup(content, 'html.parser')

        # Check for a 404 error by looking for specific text
        if soup.title and '404' in soup.title.string:
            print(f"The page at {url} is a 404 error page.")
        else:
            accessible_pages.append(page)
            print(f"The page at {url} is accessible.")

    #print(f"Total articles before filtering: {total_pages}")
    #print(f"Total articles after filtering: {len(accessible_pages)}")

    # Save the filtered accessible pages to a new file
    with open('filtered_pages.json', 'w') as f:
        json.dump(accessible_pages, f, indent=4)

    #print(f"Filtered pages saved to 'filtered_pages.json'")

with open('all_scrapped_data_combined.json', 'r') as file:
    tettra = json.load(file)

#check_page_status(tettra)

def extract_folders_and_subfolders(html_content):
    # Initialize BeautifulSoup with the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    subfolder_anchor = soup.find('a', href=lambda value: value and 'teams/company/subcategories' in value)
    if subfolder_anchor:
        match = re.search(r'teams/company/subcategories/(\d+)', subfolder_anchor['href'])
        subfolder_number = match.group(1) if match else None
    else:
        subfolder_number = None

    folder_anchor = soup.find('a', href=lambda value: value and 'teams/company/categories/' in value)
    if folder_anchor:
        match = re.search(r'teams/company/categories/(\d+)', folder_anchor['href'])
        folder_number = match.group(1) if match else None
    else:
        folder_number = None

    #print("Subfolder number:", subfolder_number)
    #print("Folder number:", folder_number)

    return folder_number, subfolder_number


#### Tettra categories
with open('', 'r') as file:
    categories_data = json.load(file)
categories_dict = {category['id']: category['name'] for category in categories_data['categories']}
folders_dict = {}

# Include folders from each category
for category in categories_data['categories']:
    for folder in category['folders']:
        folder_id = folder['id']
        parent_folder_id = folder.get('parent_folder_id')  # Get parent_folder_id if it exists

        if parent_folder_id is not None and parent_folder_id in folders_dict:
            folder_id = parent_folder_id
        folders_dict[folder_id] = folder['name']


def get_or_create_page(title, space_key, parent_id=None):
    #print(f"Creating or getting page: Title={title}, Space={space_key}, ParentID={parent_id}")
    encoded_title = requests.utils.quote(title.replace("'", "\\'"))

    # Search for existing pages with the same title in the space
    search_url = f"https://atlassian.net/wiki/rest/api/content/search?cql=space.key='{space_key}' AND title='{encoded_title}'"
    search_response = requests.get(search_url, auth=confluence_auth)
    #print(f"Search URL: {search_url}")
    #print(f"Search Response: {search_response.text}")

    if search_response.status_code == 200:
        search_results = search_response.json().get('results', [])
        if search_results:
            # If a page with the same title exists, return its ID
            existing_page_id = search_results[0]['id']
            print(f"Page with title '{title}' already exists. ID: {existing_page_id}")
            return existing_page_id
        else:
            # Prepare the page creation payload
            page_data = {
                "type": "page",
                "title": title,
                "space": {"key": space_key},
                "ancestors": [{"id": parent_id}] if parent_id else []
            }
            create_response = requests.post("https://atlassian.net/wiki/rest/api/content", json=page_data, auth=confluence_auth)
            print(f"Create Response: {create_response.text}")

            if create_response.status_code in [200, 201]:
                new_page_id = create_response.json().get('id')
                print(f"Page created successfully. New Page ID: {new_page_id}")
                return new_page_id
            elif create_response.status_code == 400:
                print(f"Page with title '{title}' already exists. Skipping creation.")
                return None
            else:
                raise Exception(f"Failed to create page: {create_response.status_code}")
    else:
        raise Exception(f"Failed to search for page: {search_response.status_code}")

def normalize_whitespace(text):
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'^\s+', ' ', text, flags=re.MULTILINE)
    text = re.sub(r'\s+$', ' ', text, flags=re.MULTILINE)
    return text

def clean_json(article):

    if 'content' in article:
        article['content'] = article['content'].strip()
        article['content'] = normalize_whitespace(article['content'].strip())

    if 'sections' in article:
        for section in article['sections']:
            if 'text' in section:
                section['text'] = section['text'].strip()
                section['text'] = normalize_whitespace(section['text'].strip())
    return article

def clean_html_structure(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    for tag in soup.find_all(text=True):
        stripped_text = tag.strip()
        tag.replace_with(stripped_text)
    for element in soup(["script", "style", "link", "br"]):
        element.decompose()
    for div in soup.find_all('div'):
        div.name = 'p'

    return str(soup)

with open('', 'r') as file:
    tettra_article = json.load(file)

    for article in tettra_article:
        article = clean_json(article)
        html_content = article['content']
        html_content = clean_html_structure(html_content)
        owned_by = None
        owned_by = extract_owned_by([article])
        print(owned_by)
        print(type(owned_by))
        urls = extract_url(article, 'extracted_url.txt')

        for url in urls:
            confluence_url = encode_url_for_confluence(url)
            confluence_content = html_content.replace(url, confluence_url)

        url = article['url']
        folder, subfolder = extract_folders_and_subfolders(html_content)
        #print("path found:", url, folder, subfolder)
        article = {
            'url': article['url'],
            'folder_id': folder,
            'subfolder_id': subfolder,
            'owned_by': owned_by,
            'content': article['content']
        }

        folder_id = int(article.get('folder_id', -1)) if article.get('folder_id') else -1
        subfolder_id = int(article.get('subfolder_id', -1)) if article.get('subfolder_id') else -1

        if folder_id in categories_dict:
            article['category_name'] = categories_dict[folder_id]
        if subfolder_id in folders_dict:
            article['subfolder_name'] = folders_dict[subfolder_id]
        else:
            article['subfolder_name'] = 'Uncategorized'

        folder_name = article['category_name']
        subfolder_name = article['subfolder_name']

        #print(article)
        confluence_url = "https://atlassian.net/wiki/rest/api/content"
        confluence_auth = (email, confluence_API)

        folder_page_id = get_or_create_page(folder_name, space_key)
        subfolder_page_id = get_or_create_page(subfolder_name, space_key, parent_id=folder_page_id)

        tettra_article = article


        content = tettra_article.get('content', '')

        # Extract the TITLE of the article
        start_index = content.find("<title>") + len("<title>")
        end_index = content.find("</title>")

        # Extract the ARTICLE
        title = content[start_index:end_index].strip()
        pattern = re.compile(r'\s*-\s*company\s*group\s*-\s*tettra\s*', re.IGNORECASE)
        updated_title = re.sub(pattern, '', title).strip()
        print("Updated Title:", updated_title)
        title = updated_title

        #print(f"Title: {title}\nContent: {content[:100]}...")
        soup = BeautifulSoup(content, 'html.parser')
        for script in soup(["script", "style"]):
            script.extract()
        draft_editor_content = soup.find('div', class_='public-DraftEditor-content')
        article_html = str(draft_editor_content)
        converted_html = convert_html_to_confluence_format(article_html)
        owned_by = '5daf1a205480c10c3357aa7d'

        #print(converted_html)
        ownership_data = {
            "type": "user",
            "username": owned_by
        }

        # Sends data to CONFLUENCE
        confluence_data = {
            "type": "page",
            "title": title,
            "space": {
                "key": space_key
            },
            "ancestors": [{"id": subfolder_page_id}],
            "body": {
                "storage": {
                    "value": converted_html,
                    "representation": "storage"
                }
            },
            "metadata": {
                "properties": {
                    "editor": {
                        "value": "v4"}
                }
            }                }
        # Confluence API credentials and endpoint
        confluence_url = "https://atlassian.net/wiki/rest/api/content"
        confluence_auth = (email, confluence_API)

        # Send the POST request to Confluence
        response = requests.post(confluence_url, json=confluence_data, auth=confluence_auth)

        # Check the response
        if response.status_code == 200:
            current_version = 1
            print(response.text)
            new_page_id = response.json().get('id')
            print(f"Page created successfully! {new_page_id}")
        else:
            print(f"Failed to create page. Status code: {response.status_code}")
            try:
                print(response.json())
            except json.JSONDecodeError:
                print("Response content is not valid JSON.")
                print(response.text)

        confluence_url = f"https://atlassian.net/wiki/api/v2/pages/{new_page_id}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        update_data = {
            "id": new_page_id,
            "status": "current",
            "title": title,
            "body": {
                "representation": "storage",
                "value": converted_html
            },
            "version": {
                "number": 2,
                "message": "updating page"},
            "ownerId": owned_by
        }
        update_data_json = json.dumps(update_data)
        response = requests.put(confluence_url, headers=headers, data=update_data_json, auth=confluence_auth)
        if response.status_code == 200:
            print(response.text)
            print(f"Ownership transferred successfully to {owned_by}.")
        else:
            print(f"Failed to transfer ownership. Status code: {response.status_code}")
            print(response.text)

    else:
        print("The list is empty.")


