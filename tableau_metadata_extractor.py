import zipfile
import xml.etree.ElementTree as ET
from collections import OrderedDict
import re
import pandas as pd
import json
import subprocess
import sys
import time
import logging
import requests
import os


token, site_id, auth_header, auth_header_xml = auth_tableau()

codes = ['123457']

def download_workbook(code, site_id, auth_header_xml):
    """
    Log into the Tableau server and download a workbook using the new authentication method.
    :param code: The workbook ID to be downloaded.
    :return: Path to the downloaded workbook.
    """
    # First authentication attempt
    # Download the workbook
    response = requests.get(
        f'https://tableau.com/api/3.15/sites/{site_id}/workbooks/{code}/content',
        headers=auth_header_xml
    )

    # Check if the response is not 200
    if response.status_code != 200:
        token, site_id, auth_header, auth_header_xml = auth_tableau()  # Retry authentication
        response = requests.get(
            f'https://tableau.com/api/3.15/sites/{site_id}/workbooks/{code}/content',
            headers=auth_header_xml
        )

    # Save the workbook
    sandbox_dir = os.path.join(os.getcwd(), 'workbook_sandbox_stary')
    os.makedirs(sandbox_dir, exist_ok=True)

    workbook_name = f'workbook_{code}'
    workbook_extension = '.twbx' if 'content' in response.headers['Content-Type'] else '.twb'
    workbook_name_with_extension = os.path.join(sandbox_dir, workbook_name + workbook_extension)

    with open(workbook_name_with_extension, 'wb') as f:
        f.write(response.content)

    return workbook_name_with_extension

def download_workbook(code, site_id, auth_header_xml):
    """
    Log into the Tableau server and download a workbook using the new authentication method.
    :param code: The workbook ID to be downloaded.
    :return: Path to the downloaded workbook.
    """
    # First authentication attempt
    # Download the workbook
    response = requests.get(
        f'https://tableau.com/api/3.15/sites/{site_id}/workbooks/{code}/content',
        headers=auth_header_xml
    )

    # Check if the response is not 200
    if response.status_code != 200:
        token, site_id, auth_header, auth_header_xml = auth_tableau()  # Retry authentication
        response = requests.get(
            f'https://tableau.com/api/3.15/sites/{site_id}/workbooks/{code}/content',
            headers=auth_header_xml
        )

    # Save the workbook
    sandbox_dir = os.path.join(os.getcwd(), 'workbook_sandbox_stary')
    os.makedirs(sandbox_dir, exist_ok=True)

    workbook_name = f'workbook_{code}'
    workbook_extension = '.twbx' if 'content' in response.headers['Content-Type'] else '.twb'
    workbook_name_with_extension = os.path.join(sandbox_dir, workbook_name + workbook_extension)

    with open(workbook_name_with_extension, 'wb') as f:
        f.write(response.content)

    return workbook_name_with_extension

class Tableau:
    def __init__(self, twbx_path=None):
        self.in_memory_files = {}
        self.twbx_path = twbx_path
        self.extracted_data = {}
        self.tree = None
        self.root = None
        self.datasource_info = {}
        self.directory = os.path.dirname(twbx_path)
        self.renamed_columns = {}

    def identify_renamed_columns(self):
        self.renamed_columns = {}
        for style_rule in self.root.findall(".//style-rule"):
            format_tag = style_rule.find(".//format[@attr='title']")
            if format_tag is not None:
                renamed_column_name = format_tag.get('value')
                original_column_identifier = format_tag.get('field')
                self.renamed_columns[original_column_identifier] = renamed_column_name

    def unpack_twbx(self):
        """
        Unpacks workbook files downloaded with workbook_luid from tableau serves, checks whether it is casual .twb files or whether it is zip file / file containing image {.twbx].
        If yes, it unpacks the file and save as "dec_workbook_luid_oded" xml file.
        :return: either .twb file or unpacked "dec_workbook_luid_oded" xml file.
        """
        # save_file_name = f"dec_{os.path.splitext(os.path.basename(self.twbx_path))[0]}_oded.xml"
        save_file_name = os.path.join(self.directory,
                                      f"dec_{os.path.splitext(os.path.basename(self.twbx_path))[0]}_oded.xml")

        file_extension = os.path.splitext(self.twbx_path)[1]
        if zipfile.is_zipfile(self.twbx_path):
            with zipfile.ZipFile(self.twbx_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith('.twb'):
                        with zip_ref.open(file_info.filename) as file:
                            file_content = file.read().decode('utf-8')
                            with open(save_file_name, 'w', encoding='utf-8') as f:
                                f.write(file_content)
                            try:
                                self.tree = ET.ElementTree(ET.fromstring(file_content))
                                self.root = self.tree.getroot()
                                for elem in self.root.iter():
                                    print(elem.tag,
                                          elem.attrib)
                            except ET.ParseError:
                                print("XML parsing failed. Skipping this file.")
                    else:
                        print(f"Skipping {file_info.filename} as it is not a .twb file.")

        elif file_extension == '.twb':
            try:
                self.tree = ET.parse(self.twbx_path)
                self.root = self.tree.getroot()
                print(f"I've lovingly parsed the content of {self.twbx_path}!")
            except ET.ParseError:
                print("XML parsing failed for the twb file.")
        else:
            print("I'm not sure what this file type is, honey.")

        print(f"I've unpacked content of {self.twbx_path} into memory!")

    def extract_info_from_twb(self):
        """
        Create and fill "Datasource" under each dashboard.
        Create and fill "Columns" under each dashboard.
        Create "GeneralInfo" and put "Datasource" and "Columns" under "GeneralInfo" dictionary.
        :return: Json filled with "GeneralInfo": "Datasource", "Columns"
        """

        for elem in self.tree.iter('repository-location'):
            report_name = elem.attrib.get('id', 'N/A')

            self.extracted_data['report_name'] = report_name
            self.extracted_data['site'] = elem.attrib.get('site', 'N/A')
            break

        self.extracted_data['Views'] = []
        self.extracted_data['Data sources'] = []

        for datasource in self.root.iter('datasource'):
            for column in datasource.iter('column'):
                column_data = {
                    "View name": column.attrib.get('caption'),
                    "computation": column.attrib.get('name'),
                    "Type": column.attrib.get('type'),
                    "Options": [member.attrib.get('value') for member in column.iter('member')]
                }
                self.extracted_data['Views'].append(column_data)
            break

        self.extracted_data['Data sources'] = []
        something = True

        for i, datasource in enumerate(self.root.iter('datasource')):
            for child in datasource:
                print("TAGS")
                print(child.tag, child.attrib)
            if 'hasconnection' in datasource.attrib:
                print("Skipping datasource with hasconnection attribute.")
                continue
            if 'caption' in datasource.attrib and 'name' in datasource.attrib:
                print(f"Debugging datasource: {datasource.attrib}")
                repository_location = datasource.find('./repository-location')
                print(f"Repository location: {repository_location}")
                id = datasource.find('.//id')
                path = datasource.find('.//path')
                sqlproxy = datasource.attrib.get('name')
                connection_elem = datasource.find('./connection')

                if connection_elem is not None:
                    classs = connection_elem.attrib.get('class')
                    dbname = connection_elem.attrib.get('dbname')
                else:
                    named_conn_elem = datasource.find('./connection/named-connections/named-connection/connection')
                    if named_conn_elem is not None:
                        print(f"Named connection element: {named_conn_elem}")
                        classs = named_conn_elem.attrib.get('class')
                        dbname = named_conn_elem.attrib.get('dbname')
                    else:
                        classs = "sad class"
                        dbname = "sad dbname. very sad."

                if repository_location is not None:
                    url = repository_location.attrib.get('derived-from')
                    id = repository_location.attrib.get('id', None)
                    path = repository_location.attrib.get('path', None)

                    if sqlproxy is None:
                        sqlproxy = repository_location.attrib.get('name')

                else:
                    sqlproxy = datasource.attrib.get('name')
                    datasources_elem = self.root.find('./datasources')
                    for child in datasources_elem:
                        print("Datasources elements:")
                        print(f"Tag: {child.tag}, Text: {child.attrib}")

                    if datasources_elem is not None:
                        for nested_datasource in datasources_elem.findall('./datasource'):
                            print("pjuu")
                            print(child.tag, child.attrib)
                            has_connection_elem = nested_datasource.attrib.get('hasconnection')
                            if has_connection_elem is None:
                                repository_location = datasources_elem.find('./datasource/repository-location')
                                if repository_location is not None:
                                    url = repository_location.attrib.get('derived-from')
                                    id = repository_location.attrib.get('id')
                                    path = repository_location.attrib.get('path')
                                    print(" i am here ")
                                else:
                                    url = None

                if sqlproxy is None:
                    named2 = datasource.find('./connection/named-connections/named-connection')
                    print(f"named2 {named2}")
                    if named2 is not None:
                        named2 = datasource.find('./connection/named-connections/named-connection')
                        sqlproxy = named2.attrib.get('name')
                    if dbname is None:
                        named2 = datasource.find('./connection/named-connections/named-connection/connection')
                        dbname = named2.attrib.get('dbname')

                if dbname is None:
                    named2 = datasource.find('./connection/named-connections/named-connection')
                    print(f"named2 {named2}")
                    if named2 is not None:
                        named2 = datasource.find('./connection/named-connections/named-connection')
                        sqlproxy = named2.attrib.get('name')
                    if dbname is None:
                        named2 = datasource.find('./connection/named-connections/named-connection/connection')
                        dbname = named2.attrib.get('dbname')

                datasource_info = {
                    'name': datasource.attrib.get('caption'),
                    "Sqlproxy": sqlproxy,
                    'URL': url,
                    'id': id,
                    'path': path,
                    'class': classs,
                    'dbname': dbname,
                    'Calculations': []
                }

                for datasource in self.root.iter('datasource'):
                    for calculation in datasource.iter('calculation'):
                        calc_datasource = calculation.attrib.get('datasource')
                        column_name = calculation.attrib.get('column')
                        formula = calculation.attrib.get('formula')

                        if not column_name:
                            continue

                        if not formula:
                            print(f"No formula found for {column_name}. Let's look somewhere else")
                        else:
                            calc_data = {
                                'datasource': calc_datasource,
                                'column name': column_name,
                                'formula': formula
                            }
                            datasource_info['Calculations'].append(calc_data)

                for worksheet in self.root.iter('worksheet'):
                    datasource_name = ''
                    for ds in worksheet.findall(".//datasources/datasource"):
                        if ds.attrib.get('name', '').startswith('sqlproxy'):
                            datasource_name = ds.attrib.get('name')
                            break

                    for column in worksheet.findall(".//column"):
                        column_name = column.attrib.get('name')
                        calculation_elem = column.find('./calculation')
                        column_caption = column.attrib.get('caption')
                        column_role = column.attrib.get('role')
                        column_type = column.attrib.get('type')
                        print(f"worksheet column names is {column_name}")
                        if calculation_elem is not None:
                            formula = calculation_elem.attrib.get('formula')
                            if formula:
                                print(f"Found the formula inside the worksheet! ")
                                calc_data = {
                                    'column name': column_name,
                                    'datasource': datasource_name,
                                    'formula': formula
                                }
                                datasource_info['Calculations'].append(calc_data)
                        print("i am before if dashboards")
                        if 'Dashboards' not in self.extracted_data:
                            self.extracted_data['Dashboards'] = {}
                            print("initialized?")

                            for dashboard_name, dashboard_content in self.extracted_data['Dashboards'].items():
                                print("inside for loop")
                                if 'Columns' not in dashboard_content:
                                    self.extracted_data['Dashboards'][dashboard_name]['Columns'] = []
                                    print("columns initialized")

                                column_data = {
                                    'name': column_name,
                                    'caption': column_caption,
                                    'role': column_role,
                                    'type': column_type,
                                    'formula': formula
                                }
                                print("Going to populate.")
                                self.extracted_data['Dashboards'][dashboard_name]['Columns'].append(column_data)
                                print("Populated.")

                datatype_mapping = {}

                for metadata_record in datasource.iter('metadata-record'):
                    local_name_element = metadata_record.find('./local-name')

                    if local_name_element is not None:
                        local_name = local_name_element.text
                        attribute_element = metadata_record.find('./attributes/attribute[@name="formula"]')

                        if attribute_element is not None:
                            datatype_mapping[local_name] = attribute_element.attrib.get('datatype')

                class_mapping = {}
                for metadata_record in datasource.iter('metadata-record'):
                    remote_name_element = metadata_record.find('./local-name')

                    if remote_name_element is not None:
                        remote_name = remote_name_element.text
                        class_value = metadata_record.attrib.get('class')
                        class_mapping[remote_name] = class_value

                for calc in datasource_info['Calculations']:
                    column_name = calc['column name']
                    if column_name in datatype_mapping:
                        calc['datatype'] = datatype_mapping[column_name]
                    if column_name in class_mapping:
                        calc['class'] = class_mapping[column_name]

                self.extracted_data['Data sources'].append(datasource_info)
                break

        repository_location_elements = self.root.iter('repository-location')
        if 'Data sources' in self.extracted_data and self.extracted_data['Data sources']:
            for data_source in self.extracted_data['Data sources']:
                if data_source.get('id') is None or data_source.get('path') is None:
                    for elem in repository_location_elements:
                        data_source['id'] = elem.attrib.get('id', None)
                        data_source['path'] = elem.attrib.get('path', None)

        if 'GeneralInfo' in self.extracted_data and self.extracted_data['GeneralInfo']:
            for info in self.extracted_data['GeneralInfo']:
                repository_location = self.root.find('./repository-location')
                if repository_location is not None:
                    if info['url'] is None:
                        url = repository_location.attrib.get('derived-from', None)
                        info['url'] = url
                else:
                    print("No repository-location found.")

        print(f"Yay! I've extracted the data and stored it in our memory! ")
        self.identify_renamed_columns()
        print(self.renamed_columns)
        if self.renamed_columns:
            if self.extracted_data.get('Views'):
                for view in self.extracted_data['Views']:
                    bublifuk_name = view.get('computation', '').strip('[]')
                    pattern = re.compile(rf".*\[none:{re.escape(bublifuk_name)}:nk\]")

                    renaming_occurred = False

                    for full_key, renamed_name in self.renamed_columns.items():
                        if pattern.match(full_key):
                            print("true")
                            view['View name'] = renamed_name
                            renaming_occurred = True
                            break

                    if not renaming_occurred:
                        print(f"No renaming found for {bublifuk_name}")
        else:
            print("No renaming needed for this dashboard.")

    def extract_worksheet(self):
        column_metadata = []

        for worksheet in self.root.iter('worksheet'):
            for table in worksheet.iter('table'):
                for view in table.iter('view'):
                    for datasource_dependencies in view.iter('datasource-dependencies'):
                        for column in datasource_dependencies.iter('column'):
                            # Only extract columns that have a 'caption' attribute
                            if 'caption' in column.attrib:
                                if 'pivot' not in column.attrib and 'derivation' not in column.attrib:
                                    column_info = {
                                        'caption': column.attrib['caption'],  # Extract caption
                                        'formula': ''
                                    }

                                    # Look for the 'calculation' tag to extract the formula
                                    calculation = column.find('calculation')
                                    if calculation is not None and column_info is not None:
                                        classa = calculation.attrib.get('class')
                                        if classa is not None:
                                            column_info['formula'] = calculation.attrib.get('formula', '')
                                    if column_info['formula'] == '':
                                        del column_info['caption']
                                        del column_info['formula']
                                    if column_info:
                                        column_info['worksheet'] = 'yes'
                                        column_metadata.append(column_info)
        return column_metadata

    def extract_all_columns(self):
        column_metadata = []
        logging.error("started logging")
        columns = self.root.findall('.//column')  # Find all columns first
        total_columns = len(columns)  # Store the total number of columns
        processed_columns = 0  # Track the number of processed columns
        logging.debug(f" total coluns is {total_columns}")
        for col in self.root.findall('.//column'):

            logging.error(f"{col.attrib}")
            if ('name' in col.attrib and 'formula' not in col.attrib):
                logging.error(f"found something {col.attrib.get('name')}")
                new_row = {
                    'caption': col.attrib.get('caption', None),
                    'formula': None,
                    'aggregation': col.attrib.get('aggregation', None),
                    'datatype': None,
                    'default-type': None,
                    'name': col.attrib.get('name', None),
                    'role': None,
                    'datasource': None,
                    'worksheet': 'else'
                }
                column_metadata.append(new_row)


            processed_columns += 1
            if processed_columns >= total_columns:
                logging.error("All columns processed. Stopping the loop.")
                print("All column processesd. Stopping loop")
                break

        return column_metadata

    def extract_only_datasource(self):
        column_metadata = []
        logging.error("Started logging")

        # Find all datasources and iterate through them
        for col in self.root.findall('.//datasource'):
            logging.error(f"Datasource attributes: {col.attrib}")

            # Iterate over all metadata-records under the current datasource
            for metadata_record in col.findall('.//metadata-record'):
                logging.debug(f"{metadata_record.attrib} metadata found")
                if metadata_record.get('class') == 'measure':
                    remote_name = metadata_record.find('remote-name').text if metadata_record.find('remote-name') is not None else None
                    logging.debug(f"{remote_name} remote name found")
                    local_name = metadata_record.find('local-name').text if metadata_record.find('local-name') is not None else None
                    caption = metadata_record.find('caption').text if metadata_record.find('caption') is not None else None
                    formula = None
                    for attribute in metadata_record.findall('.//attributes/attribute'):
                        if attribute.get('name') == 'formula':
                            formula = attribute.text
                            break  # Stop once the formula is found

                    # Extract other relevant data from the datasource and metadata record
                    datasource = {
                        'aggregation': col.get('caption'),
                        'name': remote_name,
                        'datatype': local_name,
                        'caption': caption,
                        'formula': formula  # Might be None if not found
                    }

                    column_metadata.append(datasource)

        logging.debug(f"Extracted {len(column_metadata)} column metadata records.")
        return column_metadata

    def extract_columns_metadata(self, element, existing_columns):
        for child in element:
            if child.tag == 'column':
                column_data = {
                    'caption': child.attrib.get('caption'),
                    'datatype': child.attrib.get('datatype'),
                    'name': child.attrib.get('name'),
                    'role': child.attrib.get('role'),
                    'calculation': child.attrib.get('calculation'),
                    'formula': child.find('./calculation').attrib.get('formula') if child.find(
                        './calculation') is not None else None
                }
                existing_columns.append(column_data)

    def dashboard_basic_info(self):
        """
        Add dashboard details - class, name, url, sqlproxy.
        :return: Updated json file with dashboard details.
        """
        dashboard_data = {'Dashboards': {}}

        for window in self.root.iter('window'):
            print(f"Checking window: {window.attrib}")
            if window.attrib.get('class') == 'dashboard':
                print("Found a dashboard class!")
                dashboard_name = window.attrib.get('name')
                dashboard_data['Dashboards'][dashboard_name] = {"class": 'dashboard', "name": dashboard_name}
            elif window.attrib.get('class') == 'worksheet':
                worksheet_name = window.attrib.get('name')
                dashboard_data['Dashboards'][worksheet_name] = {"class": 'dashboard', "name": worksheet_name}

        if 'Dashboards' not in self.extracted_data:
            self.extracted_data['Dashboards'] = {}

        for dashboard in self.root.iter('dashboard'):
            dashboard_name = dashboard.attrib.get('name')

            if dashboard_name in self.extracted_data['Dashboards']:
                repo_loc = dashboard.find('repository-location')
                if repo_loc is not None:
                    url = repo_loc.attrib.get('derived-from')
                    self.extracted_data['Dashboards'][dashboard_name]["url"] = url

                datasources = dashboard.find('datasources')
                if datasources is not None:
                    print("Found datasources tag.")
                    for datasource in datasources:
                        name_ = datasource.attrib.get('name')
                        print(f"Checking datasource: {name_}")
                        if name_ and name_.startswith('sqlproxy'):
                            print("Found the right one!")
                            self.extracted_data['Dashboards'][dashboard_name]["id"] = name_

                            break
        self.extracted_data['Dashboards'].update(dashboard_data['Dashboards'])
        print(f"Data added to the in-memory storage: {self.extracted_data['Dashboards']}")

        print("Yay!  Your in-memory storage is now updated with all the dashboard details! ")

    def add_datasource_dependencies(self):
        """
        Add Views with their options into json. E.G. View "Time scale", options: "Weekly", "Monthly", "Total".
        :return: Updated json with Views and their specifics.
        """
        if 'Dashboards' not in self.extracted_data:
            self.extracted_data['Dashboards'] = {}
            print("Initialized Dashboards in extracted_data.")  #

        for window in self.root.iter('window'):
            window_class = window.attrib.get('class')
            print(f"Checking window: {window.attrib}")
            for dashboard in self.root.iter(window_class):
                dashboard_name = dashboard.attrib.get('name')
                print(f"Processing dashboard: {dashboard_name}")  # Debugging line
                if dashboard_name not in self.extracted_data.get('Dashboards', {}):
                    continue  # Skip if the dashboard is not in the JSON
                self.extracted_data['Dashboards'][dashboard_name]['Views'] = []
                if dashboard_name not in self.extracted_data.get('Dashboards', {}):
                    self.extracted_data['Dashboards'][dashboard_name] = {'Views': []}  # Add the dashboard to JSON
                if not isinstance(self.extracted_data['Dashboards'][dashboard_name].get('Views', None), list):
                    self.extracted_data['Dashboards'][dashboard_name]['Views'] = []

                for datasource_dependency in dashboard.findall('.//datasource-dependencies'):
                    for column in datasource_dependency.findall('.//column'):
                        param_domain_type = column.attrib.get('param-domain-type', '')
                        aggregation_type = column.attrib.get('aggregation', None)
                        caption = column.attrib.get('caption', '')

                        if param_domain_type == 'list' and aggregation_type is None:
                            value = column.attrib.get('value', '')
                            type = column.attrib.get('type', 'not Found')
                            options = [m.attrib['value'] for m in column.findall('.//member')]

                            # Append the view data to 'Views'
                            self.extracted_data['Dashboards'][dashboard_name]['Views'].append({
                                'caption': caption,
                                'type': type,
                                'value': value,
                                'options': options
                            })
                            print(
                                f"Last appended to Views: {self.extracted_data['Dashboards'][dashboard_name]['Views'][-1]}")
            print("Voila! The JSON is updated with datasource dependencies!")

    def add_column_details(self):
        """
        Update "Coluns" with details, such as aggregation, caption, datatype, default-type, name, role.
        :return: Updated json with column details.
        """
        if 'Dashboards' not in self.extracted_data:
            self.extracted_data['Dashboards'] = {}
        json_dashboard_names = set(self.extracted_data.get("Dashboards", {}).keys())
        print(f"json dashboard names: {json_dashboard_names}")

        for window in self.root.iter('window'):
            window_class = window.attrib.get('class')
            print(f"Checking window: {window.attrib}")
            for dashboard in self.root.iter(window_class):
                dashboard_name = dashboard.attrib.get('name')
                print(f"dashboard names are {dashboard_name}")
                for dashboard_elem in self.root.findall(f'.//{window_class}'):
                    print(f"Dashboard element attributes: {dashboard_elem.attrib}")
                    elem_name = dashboard_elem.attrib.get('name', 'Unnamed')
                    print(f"Element name is: {elem_name}")  # Debug line
                    # Debug line
                    if 'name' in dashboard_elem.attrib and dashboard_elem.attrib['name'] in json_dashboard_names:
                        print("I am inside for loop.")
                        sqlproxy_name = ""
                        for ds in dashboard_elem.findall(".//datasource"):
                            ds_name = ds.attrib.get("name", "")
                            if ds_name.startswith("sqlproxy"):
                                sqlproxy_name = ds_name
                                print(sqlproxy_name)
                                break
                        if not sqlproxy_name:
                            print("sqlproxy name not found")

                        columns_list = []
                        for column in self.root.findall(
                                f".//datasource-dependencies[@datasource='{sqlproxy_name}']/column"):
                            column_info = {
                                "aggregation": column.attrib.get("aggregation"),
                                "caption": column.attrib.get("caption"),
                                "datatype": column.attrib.get("datatype"),
                                "default-type": column.attrib.get("default-type"),
                                "name": column.attrib.get("name"),
                                "role": column.attrib.get("role", "null")
                            }
                            columns_list.append(column_info)

                        self.extracted_data["Dashboards"][dashboard_name].update({
                            "Columns": columns_list
                        })

                        if "Unknown Dashboard" in self.extracted_data["Dashboards"]:
                            print(f"Unknown Dashboard detected. Current state of extracted_data: {self.extracted_data}")
        for dashboard_name, dashboard_data in self.extracted_data.get("Dashboards", {}).items():
            columns = dashboard_data.get("Columns", [])
            if not columns:  # If 'Columns' is empty
                print(f"No columns found for dashboard: {dashboard_name}. Running specific column data processing.")
                column_data_list = []
                for column in self.root.findall(".//column"):
                    caption = column.attrib.get('caption', '')
                    datatype = column.attrib.get('datatype', '')
                    name = column.attrib.get('name', '')
                    role = column.attrib.get('role', '')
                    formula_element = column.find(".//calculation")
                    formula = formula_element.attrib.get('formula', '') if formula_element is not None else ''
                    if caption:
                        column_data = {
                            "caption": caption,
                            "datatype": datatype,
                            "name": name,
                            "role": role,
                            "formula": formula
                        }
                        column_data_list.append(column_data)

                if 'Columns' not in self.extracted_data:
                    self.extracted_data['Columns'] = []

                self.extracted_data['Columns'].extend(column_data_list)
                print(f"Updated SpecificColumns: {self.extracted_data['Columns']}")
    def clean_dashboard_columns(self):
        """
        Add "Caption" and "Options" under "Views". Updates "Columns" so only table columns are kept in there.
        :return: Updated "Views" and "Columns" dictionary in json.
        """
        view_captions_and_options = []

        for dashboard_name, dashboard_content in self.extracted_data.get("Dashboards", {}).items():
            for view in dashboard_content.get("Views", []):
                caption = view.get("caption", "").strip("\"'")
                options = view.get("options", [])
                stripped_options = [option.strip("\"'") for option in options]

                if caption:
                    view_captions_and_options.append(caption)
                if stripped_options:
                    view_captions_and_options.extend(stripped_options)

        for dashboard_name, dashboard_content in self.extracted_data.get("Dashboards", {}).items():
            cleaned_columns = [col for col in dashboard_content.get("Columns", []) if
                               col.get("caption") is not None]
            cleaned_columns = [col for col in cleaned_columns if
                               col.get("caption").strip("\"'") not in view_captions_and_options]
            dashboard_content["Columns"] = cleaned_columns

        print("Look at that! Clean and shiny columns! ")

    def merge_dashboard_columns_with_datasources(self):
        """
        Update "Columns" with name and role, get rid of rest of the "Views" in "Columns"
        :return: Updated "Columns" dict in json.
        """
        column_name_to_calculation = {}

        for datasource in self.extracted_data.get("Data sources", []):
            for calculation in datasource.get("Calculations", []):
                column_name = calculation.get("column name", "").strip("\"'")
                if column_name:
                    column_name_to_calculation[column_name] = calculation

        for dashboard_name, dashboard_content in self.extracted_data.get("Dashboards", {}).items():
            updated_columns = []
            for col in dashboard_content.get("Columns", []):
                col_name = col.get("name", "").strip("\"'")
                col_role = col.get("role", "")

                if col_role == "dimension":
                    continue

                matching_calculation = column_name_to_calculation.get(col_name)

                if matching_calculation:
                    col.update(matching_calculation)

                updated_columns.append(col)

            dashboard_content["Columns"] = updated_columns

        print("Columns and calculations are merged and updated in memory! ")

    def remove_views_and_calculations(self):
        """
        Long long time ago it used to delete whole "Views".
        Then Data Engineers came that they can not live without "Views" in Rohlikbot.
        So we kept it.
        For the sake of Data analyst.
        Now it just deletes "Calculations" from "Views".
        :return: Updated "Views" dictionary without "Calculations".
        """
        if "workbook.twb" in self.extracted_data:
            del self.extracted_data["workbook.twb"]

        # if "Views" in self.extracted_data:
        # del self.extracted_data["Views"]

        # for dashboard_content in self.extracted_data.get("Dashboards", {}).values():
        # if "Views" in dashboard_content:
        # del dashboard_content["Views"]

        for datasource in self.extracted_data.get("Data sources", []):
            if "Calculations" in datasource:
                del datasource["Calculations"]

        for dashboard_name, dashboard_content in self.extracted_data.get("Dashboards", {}).items():
            updated_columns = []
            for col in dashboard_content.get("Columns", []):
                if col.get("name") == col.get("column name"):
                    filtered_col = {k: v for k, v in col.items() if k != 'column name'}
                else:
                    filtered_col = col
                updated_columns.append(filtered_col)
            dashboard_content["Columns"] = updated_columns
        print("'Calculations' have vanished!")

    def add_url(self):
        """
        Takes care that every Dashboard, which has url in metadata, has the url also in json.
        It covers a lot of possibilities where url can be. If it is still not in the json,
        send workbook_luid and we can look. But probably the dashboard just does not have url in metadata.
        :return: Updated GeneralInfo dict in json file.
        """
        dashboards_missing_url = set()
        for window in self.root.iter('window'):
            window_class = window.attrib.get('class')
            print(f"Checking window: {window.attrib}")
            for dashboard in self.root.iter(window_class):
                name = dashboard.attrib.get('name')
                print(f"THE NAMES OF ALL {name}")
                for child in dashboard:
                    print("TAGES")
                    print(child.tag, child.attrib)

        for dashboard in self.root.findall(f".//{window_class}"):
            print(f" WINDOW CLASS IS !! {window_class} ")
            for child in dashboard:
                print(f"tag under windows")
                print(child.tag, child.attrib)

            name = dashboard.get("name")
            repo_location = dashboard.find("repository-location")
            derived_from = repo_location.get("derived-from") if repo_location is not None else None
            if derived_from is None:
                for dashboard in self.root.findall(".//dashboard"):
                    print(f"Checking dashboard: {dashboard.attrib}")
                    repo_location = dashboard.find("repository-location")
                    new_derived_from = repo_location.get("derived-from") if repo_location is not None else None
                    new_name = dashboard.get("name")
                    if new_name == name:
                        derived_from = new_derived_from
                        break
            print(f"Checking Debugging god: {derived_from}")

            if derived_from is None:
                for dashboard in self.root.iter('dashboard'):
                    repo_location = dashboard.find("./repository-location")
                    derived_from = repo_location.get("derived-from") if repo_location is not None else None

            print(f"Dashboard name: {name}, Derived From: {derived_from}")

            if name in self.extracted_data['Dashboards']:
                temp_dict = self.extracted_data['Dashboards'][name]
                new_dict = {'class': temp_dict.get('class'), 'name': temp_dict.get('name'), 'url': derived_from}
                print(f"New Dictionary: {new_dict}")

                for k, v in temp_dict.items():
                    if k not in ['class', 'name']:
                        new_dict[k] = v

                self.extracted_data['Dashboards'][name] = new_dict
                print(f"After Merge: {self.extracted_data['Dashboards'][name]}")

        for dashboard_name, dashboard_data in self.extracted_data['Dashboards'].items():
            if 'url' not in dashboard_data:
                dashboards_missing_url.add(dashboard_name)

        if dashboards_missing_url:
            for dashboard in self.root.findall(".//dashboard"):
                name = dashboard.get("name")
                if name in dashboards_missing_url:
                    repo_location = dashboard.find("repository-location")
                    derived_from = repo_location.get("derived-from") if repo_location is not None else None
                    if derived_from is not None:

                        old_dict = self.extracted_data['Dashboards'][name]
                        new_dict = {}
                        new_dict['class'] = old_dict.get('class')
                        new_dict['url'] = derived_from
                        for k, v in old_dict.items():
                            if k not in ['class', 'url']:
                                new_dict[k] = v
                        self.extracted_data['Dashboards'][name] = new_dict
                        dashboards_missing_url.remove(name)
        if dashboards_missing_url:
            for worksheet in self.root.findall(".//worksheet"):
                name = worksheet.get("name")
                if name in dashboards_missing_url:
                    repo_location = worksheet.find(".//repository-location")
                    derived_from = repo_location.get("derived-from") if repo_location is not None else None
                    if derived_from is not None:
                        old_dict = self.extracted_data['Dashboards'].get(name, {})
                        new_dict = {'class': old_dict.get('class'), 'url': derived_from}

                        new_dict.update({k: v for k, v in old_dict.items() if k not in ['class', 'url']})
                        self.extracted_data['Dashboards'][name] = new_dict
                        dashboards_missing_url.remove(name)

        print("The URLs are in! ")

    def move_general_info(self):
        """
        Takes care that GeneralInfo is in the right place with the right source of information.
        :return: Updated "GeneralInfo" dict in json.
        """
        if not self.extracted_data:
            print("Oops, no data to move around! ")
            return

        if 'Dashboards' not in self.extracted_data:
            self.extracted_data['Dashboards'] = {}

        general_info_keys = ['name', 'site', 'Data sources']
        general_info = {key: self.extracted_data.get(key) for key in general_info_keys}
        print(general_info)

        for dashboard_name, dashboard_details in self.extracted_data['Dashboards'].items():
            dashboard_details['GeneralInfo'] = {}
            print(general_info)
            for key, value in general_info.items():
                if key != 'name':
                    dashboard_details[key] = value
        if 'Dashboards' in self.extracted_data:
            for dashboard_name, dashboard_details in self.extracted_data['Dashboards'].items():

                dashboard_details['GeneralInfo'] = general_info
                dashboard_details.pop('Data sources', None)
                dashboard_details.pop('site', None)

                reordered_dashboard_details = OrderedDict()

                if 'Introduction' in dashboard_details:
                    reordered_dashboard_details['Introduction'] = dashboard_details['Introduction']

                if 'GeneralInfo' in dashboard_details:
                    reordered_dashboard_details['GeneralInfo'] = dashboard_details['GeneralInfo']
                    dashboard_details['GeneralInfo'].pop('name', None)

                for key, value in dashboard_details.items():
                    if key not in ['Introduction', 'GeneralInfo']:
                        reordered_dashboard_details[key] = value

                self.extracted_data['Dashboards'][dashboard_name] = reordered_dashboard_details

        for key in general_info_keys:
            if key in self.extracted_data:
                del self.extracted_data[key]

        print("The general info has been moved! ")

    def url_edits(self):
        """
        It probably renames url in "Data sources" so the program does not confuse it.
        :return: Renamed "url" in "Data sources" dictionary in json.
        """
        print("I am inside url edits")
        dashboards = self.extracted_data.get("Dashboards", {})
        print("Dashboards data:", dashboards)
        if not isinstance(dashboards, dict):
            raise ValueError("Dashboards data is not a dictionary or is missing")

        for dashboard_name, dashboard_data in dashboards.items():
            if not isinstance(dashboard_data, dict):
                print(f"Warning: Skipping non-dictionary dashboard data for {dashboard_name}")
                continue

            if 'url' in dashboard_data:
                url = dashboard_data['url']
                if url is not None:
                    dashboard_data['url'] = self.edit_url_directly(url)
                    print(f"Performed URL edit for dashboard {dashboard_name}")
                else:
                    print(f"No URL to edit in dashboard {dashboard_name}")

            general_info = dashboard_data.get('GeneralInfo', {})
            if not isinstance(general_info, dict):
                print(f"Warning: 'GeneralInfo' is not a dictionary for dashboard {dashboard_name}")
                continue

            data_sources = general_info.get('Data sources', [])
            if not isinstance(data_sources, list):
                print(f"Warning: 'Data sources' is not a list for dashboard {dashboard_name}")
                continue

            for data_source in data_sources:
                if not isinstance(data_source, dict):
                    print(f"Warning: Skipping non-dictionary data source in dashboard {dashboard_name}")
                    continue

                url = data_source.get('URL', None)
                if url is not None:
                    data_source['URL'] = self.edit_url_directly(url)
                    print(f"Performed URL edit for data source in dashboard {dashboard_name}")
                else:
                    print(f"No URL to edit in data source for dashboard {dashboard_name}")

            print("Completed processing for this dashboard")


    def edit_url_directly(self, url):
        """
        In the metadata, there are wrongly formatted urls. This function rewrites them so they are executable.
        :param url:  Rewrites workbooks into views, deletes ?rev= in the end of url and rewrites localhost into 10.20.0.20.
        :return: Rewritten "url" in "GeneralInfo" dictionary in final json.
        """
        # if '/t/' in url:
        #  url = url.replace('/t/', '/#/')

        if '/workbooks/' in url:
            url = url.replace('/workbooks/', '/views/')

        rev_index = url.find('?rev=')
        if rev_index != -1:
            url = url[:rev_index]

        url = re.sub(r'localhost:\d+/', '10.20.0.20/', url)
        if 'http://10.20.0.20/' in url:
            url = url.replace('http://10.20.0.20/', 'http://tableau.com/')

        return url

    def save_data(self):
        """
        Creates file "TABLEAUDATAZEXTRAKTORU", which contains all of the scraped metadata information.
        Then go three folders backward and save it into 'langchain'
        :return: "TABLEAUDATAZEXTRAKTORU" final file in langchain folder.
        """
        # destination_directory = os.path.dirname(self.twbx_path)
        # destination_directory = 'in/files/'

        file_path = 'TABLEAUDATAZEXTRAKTORU_test.json'
        # file_path = os.path.join(destination_directory, json_file_name)
        print("i'm here inside saving")
        if not os.path.isfile(file_path):
            with open(file_path, 'w') as f:
                json.dump([self.extracted_data], f, indent=4)
                print("json dumped")
        else:
            with open(file_path, 'r') as f:
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        data.append(self.extracted_data)
                        print("data appended")
                    else:
                        data = [data, self.extracted_data]
                        print("another way of saving data")
                except json.JSONDecodeError:
                    data = [self.extracted_data]

            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
                print("json dumped finally")

        print(f"Data saved to {file_path}")

for code in codes:
    retry_count = 0  # Initialize retry counter
    while retry_count < 2:  # Allow for up to 2 retries
        try:
            print(f"code is {code}")
            downloaded_workbook_name = download_workbook(code, site_id, auth_header_xml)
            print(code)
            twbx_path = os.path.join(os.getcwd(), downloaded_workbook_name)
            tableau = Tableau(twbx_path)

            tableau.unpack_twbx()
            print("Before Extracting")
            tableau.extract_info_from_twb()
            print("After extract")
            print(f"{tableau.extracted_data}")
            print(f"Extract successfull")

            for dashboard_name, dashboard_content in tableau.extracted_data['Dashboards'].items():
                if 'Columns' not in dashboard_content:
                    dashboard_content['Columns'] = []
                tableau.extract_columns_metadata(tableau.root, dashboard_content['Columns'])

            tableau.dashboard_basic_info()
            print("Before datasource dependencies")
            tableau.add_datasource_dependencies()
            print("Before adding column details")
            tableau.add_column_details()
            tableau.clean_dashboard_columns()
            tableau.merge_dashboard_columns_with_datasources()
            tableau.remove_views_and_calculations()
            tableau.add_url()
            print("Before move general info")
            tableau.move_general_info()
            print("After move general before url edits")
            tableau.url_edits()
            print("After url edits and extracted data")
            tableau.extracted_data['workbook_luid'] = code
            print("I came after extracted data")

            #print(f"Extracted data before processing: {tableau.extracted_data}")

            if not tableau.extracted_data or not isinstance(tableau.extracted_data, dict):
                raise ValueError("No valid data extracted or data is not a dictionary")
            report_name = tableau.extracted_data.get('report_name', 'Unknown')
            workbook_luid = tableau.extracted_data.get('workbook_luid', 'Unknown')
            if workbook_luid == 'Unknown':
                raise ValueError("Workbook LUID is missing")

            new_dict = {
                'report_name': tableau.extracted_data['report_name'],
                'workbook_luid': tableau.extracted_data['workbook_luid'],
            }
            dashboard_names = [key for key in tableau.extracted_data.get("Dashboards", {})]

            for key, value in tableau.extracted_data.items():
                if key not in ['report_name', 'workbook_luid']:
                    new_dict[key] = value

            tableau.extracted_data = new_dict
            print(f"Extracted data:")
            column_metadata = tableau.extract_worksheet()
            logging.debug(f"column metadata extracted successfully")
            for dashboard_name, dashboard_content in tableau.extracted_data.get("Dashboards", {}).items():
                for column in dashboard_content.get("Columns", []):
                    for col_metadata in column_metadata:
                        print(f"Comparing {col_metadata.get('caption')} and {column.get('caption')}")
                        if col_metadata.get('caption') == column.get('caption'):
                            column['worksheet'] = 'yes'
                            print(f"for  {col_metadata.get('caption')} and {column.get('caption')} decision is yes")
                            break
                        else:
                            column['worksheet'] = 'no'
            all_columns = tableau.extract_all_columns()
            for i, (dashboard_name, dashboard_content) in enumerate(tableau.extracted_data.get("Dashboards", {}).items()):
                if i == 0:
                    dashboard_content["Columns"].extend(all_columns)
                    logging.debug(f"Columns successfully saved")
            if code in ['code1', 'code2']:
                datasources = tableau.extract_only_datasource()
                for i, (dashboard_name, dashboard_content) in enumerate(tableau.extracted_data.get("Dashboards", {}).items()):
                    if i == 0:
                        dashboard_content["Columns"].extend(datasources)
                        logging.debug(f"Datasources !!! Data Sources !!  successfully saved")
            logging.debug(f"before saving data")
            tableau.save_data()
            print("Processing complete for code:", code)
            break

        except Exception as e:
            print(f"An error occurred with code {code}: {e}.")
            retry_count += 1  # Increment retry counter
            if retry_count < 2:  # Only wait if we have retries left
                print("Waiting 15 seconds to retry...")
                time.sleep(15)
            else:
                print("Max retries reached. Skipping to the next code.")

import pandas as pd
import json

# Load the JSON data from file
with open('TABLEAUDATAZEXTRAKTORU_test.json', 'r') as file:
    data = json.load(file)

if isinstance(data[0], str):
    data = [json.loads(item) if isinstance(item, str) else item for item in data]

#print(f"First item in data after parsing (if string): {data[0]}")
#print(f"Type of first item now: {type(data[0])}")  # Should be a dict

print("json successfully loaded")

# Initialize empty lists for data collection
dashboard_list_data = []
dashboards_data = []
views_data = []
columns_data = []
datasources_data = []
extracted_data = []

extracted_data = []

for item in data:
    columns = item.get('Columns', [])
    for column in columns:
        extracted_data.append({
            'name': item.get('name'),
            'url': item.get('url'),
            'caption': column.get('caption', ''),
            'datatype': column.get('datatype', ''),
            'name_column': column.get('name', ''),
            'role': column.get('role', ''),
            'formula': column.get('formula', ''),
            'worksheet': column.get('worksheet', '')
        })

all_columns_new = pd.DataFrame(extracted_data)
all_columns_new.to_csv('all_columns_wb.csv')

# Extract data
for entry in data:
    workbook_luid = entry['workbook_luid']
    report_name = entry['report_name']

    # Iterate through each dashboard within an entry
    for dashboard_key, dashboard_value in entry['Dashboards'].items():
        #print(entry['Dashboards'].items())
        # Construct wbldbn identifier
        wbldbn = f"{workbook_luid}_{dashboard_key}"

        # Dashboard list data collection
        dashboard_list_data.append({
            'workbook_luid': workbook_luid,
            'report_name': report_name,
            'dashboard_name': dashboard_key,
            'wbldbn': wbldbn
        })

        site = dashboard_value['GeneralInfo'].get('site', 'N/A')
        dashboard_class = dashboard_value.get('class', 'N/A')  # Safe access if 'class' might not be present
        url = dashboard_value.get('url', 'N/A')  # Safe access for 'url'

        # Safely extract 'Columns', 'Views', and 'Data sources' if they exist
        column_names = [col.get('name', 'N/A') for col in
                        dashboard_value.get('Columns', [])]  # Default to empty list if not found
        view_captions = [view.get('caption', 'N/A') for view in
                         dashboard_value.get('Views', [])]  # Default to empty list if not found
        datasource_ids = [ds.get('id', 'N/A') for ds in
                          dashboard_value['GeneralInfo'].get('Data sources', [])]  # Default to empty list if not found

        dashboard_row = {
            'wbldbn': wbldbn,
            'site': site,
            'class': dashboard_class,
            'url': url
        }

        dashboards_data.append(dashboard_row)
        # Views data collection
        for view in dashboard_value['Views']:
            views_data.append({
                'wbldbn': wbldbn,
                'caption': view['caption'],
                'type': view['type'],
                'value': view.get('value', ''),
                'options': ', '.join(view.get('options', []))
            })

        # Columns data collection
        for column in dashboard_value['Columns']:
            columns_data.append({
                'wbldbn': wbldbn,
                'aggregation': column.get('aggregation', None),
                'caption': column.get('caption', None),
                'datatype': column.get('datatype', None),
                'default-type': column.get('default-type', None),
                'name': column.get('name', None),
                'role': column.get('role', None),
                'formula': column.get('formula', None),
                'worksheet': column.get('worksheet', None)
            })

        # Datasources data collection
        for datasource in dashboard_value['GeneralInfo']['Data sources']:
            datasources_data.append({
                'wbldbn': wbldbn,
                'id': datasource['id'],
                'name': datasource['name'],
                'sqlproxy': datasource.get('Sqlproxy', ''),  # Use get for optional fields
                'url': datasource.get('URL', 'N/A'),  # missing url just to be sure
                'dbname': datasource['dbname']
            })

# Create DataFrames
workbooks = pd.DataFrame(dashboard_list_data)
views = pd.DataFrame(dashboards_data)
filters = pd.DataFrame(views_data)
columns = pd.DataFrame(columns_data)
datasources = pd.DataFrame(datasources_data)

workbooks.to_csv('workbooks.csv', index=False)
views.to_csv('views.csv', index=False)
filters.to_csv('filters.csv', index=False)
columns.to_csv('columns.csv', index=False)
datasources.to_csv('datasources.csv', index=False)

print("dataframes are ready to map")