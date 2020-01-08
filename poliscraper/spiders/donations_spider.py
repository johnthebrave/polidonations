
from scrapy import Request, Spider
from util import load_dataset, dump_dataset
from data.ro_parties import parties

from pprint import pprint as pp
import re

from collections import defaultdict
from util import print_defaultdict

class MO_LIST(Spider):

    name = "monitorul_oficial_list"
    allowed_domains = ["www.monitoruljuridic.ro"]

    start_urls = ["http://www.monitoruljuridic.ro/acte-institutii/partide-politice/{}".format(i) for i in range(1, 33)]

    targets = []

    def parse(self, response):

        # Pagination
        # next = response.xpath(u"//div[@class='pagin']/a[text()='›']/@href").extract_first()
        # if next:
        #     yield Request(next, self.parse, dont_filter=True)
        links = response.xpath("//div[@class='mo_intro']/h3/a/@href").extract()
        for link in links:
            self.targets.append(link)

    def closed(self, spider):
        dump_dataset("./data/mo_list.json", self.targets)

class MO_PAGE(Spider):

    name = "monitorul_oficial"
    allowed_domains = ["www.monitoruljuridic.ro"]

    unidentified = []
    all_column_types = defaultdict(int)

    all_donations = []

    def start_requests(self):

        targets = load_dataset("./data/mo_list.json")
        for target in targets:
            yield Request(target, self.parse)

    def get_title(self, response):

        title = response.xpath("//div[@id='container']/div[@class='box']/div[3]/h1/text()").extract_first()

        if not "CUANTUM" in title and not "RECTIFICARE" in title:
            self.unidentified.append((response.url, "Error parsing title"))
            return None

        return title

    def get_date(self, response):

        date = response.url.split("-")
        if date[5].isdigit() and len(date[5]) == 4:
            date = str(int(date[5]) - 1)
        elif date[3].isdigit() and len(date[3]) == 4:
            date = str(int(date[3]) - 1)
        else:
            self.unidentified.append((response.url, "Error parsing date"))
            return None

        return date

    def get_content(self, response):

        content = response.xpath("//div[@class='mo_intro']/following-sibling::div")
        if not content:
            self.unidentified.append((response.url, "No content"))
            return None

        return content

    def identify_party(self, content):
        # Warning: Party list is old! (e.g. USR - corresponds to Uniunea Sarbilor din Romania)
        # Warning: typo's are possible!

        for party in parties:

            if party["name"] in content:
                return party["name"]

            if "alias" in party:
                for alias in party["alias"]:
                    if len(alias) > 0 and alias in content:
                        return party["name"]

            if "acronym" in party and party["acronym"] in content and len(party["acronym"]) > 0:
                # print(party)
                return party["name"]

        return None

    # Row demarcation
    def check_full_line(self, line):
        for ch in line:
            if ch not in ["├", "─", "┼", "┤", "┴"]:
                return False
        return True

    # Some tables have some irrelevant space, possibly with text, before/after
    # Check if you're within an actual table
    def check_invalid_space(self, line):

        for ch in line:
            if ch in ["├", "─", "┼", "┤", "┴", "│", "┬", "┌", "┐"]:
                return False
        return True

    def augument_column_meanings(self, splitter, table):

        num_columns    = len(splitter.split(table[1])[1:-1])
        current_idx    = 0
        column_meaning = ["" for _ in range(num_columns)]

        for idx in range(1, len(table)):

            current_idx += 1
            line = table[idx]

            if u"─" in line:
                current_idx += 1
                break

            legends = splitter.split(line)[1:-1]
            if len(legends) != num_columns:
                raise ValueError("Unexpected number of columns")

            for jdx in range(num_columns):
                legend = legends[jdx].strip().lower()

                if len(legend) == 0:
                    continue

                if len(column_meaning[jdx]) > 0 and \
                                column_meaning[jdx][-1] == u"-":
                    column_meaning[jdx] = column_meaning[jdx][:-1] + legend
                else:
                    column_meaning[jdx] += u" " + legend

        column_meaning = map(lambda x: x.strip(), column_meaning)
        return column_meaning, current_idx

    # Tables tend to end with a "TOTAL" row - ignore it
    def check_valid_entry(self, entry):
        for col in entry:
            if entry[col] and "TOTAL" in entry[col]:
                return False
        return True

    # Saves individual donations
    # Issue: Tables where the number of rows isn't the same for each entry
    def parse_individual_donations(self, current_line, columns, party, donation_type, source):

        if len(columns) == 0:
            return []

        donations = []

        for idx, cell in enumerate(current_line):

            cell = cell.strip()

            if len(cell) == 0:
                continue

            if cell[-1] == "@":
                cell = cell[:-1].strip()

            content = cell.split("@")

            if len(content) == 1:

                if len(donations) == 0:
                    donations.append({
                        columns[idx]: content[0].strip(),
                        "party": party,
                        "donation_type": donation_type,
                        "source": source
                    })
                else:
                    for entry in donations:
                        entry[columns[idx]] = content[0].strip()
            else:

                if len(donations) == 0:
                    for chunk in content:
                        donations.append({
                            columns[idx]: chunk.strip(),
                            "party": party,
                            "donation_type": donation_type,
                            "source": source
                        })
                else:
                    if len(content) != len(donations):
                        donations = [entry for entry in donations if self.check_valid_entry(entry)]

                    if len(content) != len(donations):
                        count_empty = 0
                        for inner in content:
                            if len(inner.strip()) == 0:
                                count_empty += 1

                        if not count_empty == len(content) - 1:

                            if len(donations) == 1:
                                entry = donations[0]
                                donations = []
                                for chunk in content:
                                    new_entry = dict(entry)
                                    new_entry[columns[idx]] = chunk.strip()
                                    donations.append(new_entry)
                                continue
                            else:
                                # print(content)
                                # print(donations)
                                raise Exception("Complex table")

                        content = "".join(map(lambda x: x.strip(), content))
                        content = [content] * len(donations)

                    for jdx, entry in enumerate(donations):
                        entry[columns[idx]] = content[jdx].strip()

        donations = [entry for entry in donations if self.check_valid_entry(entry)]

        return donations

    def extract_table(self, node):

        table = node.xpath("./following-sibling::div[@class='wdth'][1]/pre/text()").extract()

        if len(table) == 0:
            table = node.xpath("./following-sibling::pre[1]/span/text()").extract()

        if len(table) == 0:
            table = node.xpath("./following-sibling::div[@class='wdth'][1]/pre[1]/span/text()").extract()

        if len(table) < 2:
            table = node.xpath("./following-sibling::div[@class='wdth'][1]/pre[1]//span/text()").extract()

        if not table or len(table) < 2:
            return []

        return table


    simplified_column_names = {
        "Naţiona- litatea": "Nationalitate",
        "Felul dona- ţiei": "Felul donaţiei",
        "Naţio- nali- tatea": "Nationalitate",
        "Codul numeric personal": "CNP",
    }

    # TODO: There are still some redundant column names
    def simplify_column_name(self, column_name):

        if column_name in self.simplified_column_names:
            column_name = self.simplified_column_names[column_name]
        elif "Valoare" in column_name:
            column_name = "Valoare"
        elif "Data" in column_name:
            column_name = "Data"
        elif "Nume" in column_name:
            column_name = "Numele"
        elif "Cuantum" in column_name:
            column_name = "Valoare"
        elif "Venituri" in column_name:
            column_name = "Valoare"
        elif "Denumirea" in column_name:
            column_name = "Numele"
        elif "Codul de înregistrare fiscalã":
            column_name = "CIF"

        return column_name

    def parse_table(self, table, target, donation_type, source):

        current_line = []
        column_len   = []
        columns      = []
        result       = []
        accum        = ""
        skip         = False

        for line in table:

            if "┌" in line:
                # we have a new table
                # compute number of characters for each column
                header       = line.split("┬")
                column_len   = [len(col) for col in header]
                columns      = []

                current_line = ["" for _ in range(len(column_len) + 1)]
                accum = ""
                continue

            if len(column_len) == 0:
                continue

            line = line.strip()
            current_column = -1

            if self.check_invalid_space(line):
                continue

            if "TOTAL" in line or skip:
                skip = True
                continue

            if self.check_full_line(line):
                skip = False

                if len(columns) == 0:
                    for cell in current_line[:-1]:

                        column_name = " ".join(cell.strip().split())
                        column_name = self.simplify_column_name(column_name)
                        columns.append(column_name)

                        # for debug, count number of columns of a certain type
                        self.all_column_types[column_name] += 1
                else:
                    result += self.parse_individual_donations(current_line[:-1], columns, target, donation_type, source)

                # reset current line
                current_line = ["" for _ in range(len(column_len) + 1)]
                accum = ""
                continue

            for idx, ch in enumerate(line):

                if ch == "│" or ch == "┼" or ch == "┴" or ch == "├" or ch == "┤":
                    accum = accum.replace("─", "")

                    sep = " "
                    if ch == "├" or ch == "┼" or ch == "┴" or ch == "┤" or ch == "┼":
                        sep = " @ "

                    current_line[current_column] = accum.strip() + sep
                    current_column += 1
                    if current_column >= len(current_line):
                        break
                    accum = current_line[current_column]
                else:
                    accum += ch

        result += self.parse_individual_donations(current_line[:-1], columns, target, donation_type, source)

        return result

    # Page Parsing Starts Here
    def parse(self, response):

        title = self.get_title(response)
        if not title:
            return

        date = self.get_date(response)
        if not date:
            return

        content = self.get_content(response)
        if not content:
            return

        spans = content.xpath("./*")
        party = None

        for span in spans:

            text = span.xpath("./text()").extract_first()
            if not text:
                continue

            new_party = self.identify_party(text)

            if new_party is not None:
                party = new_party
            else:
                text  = text.lower()

                if "membrilor de partid" in text or \
                   "membri de partid" in text:
                    donation_type = "membri de partid"
                elif "persoanelor fizice" in text or \
                     "persoane fizice" in text or \
                     "persoanele fizice" in text:
                    donation_type = "persoane fizice"
                elif "persoanelor juridice" in text or \
                     "persoane juridice" in text:
                    donation_type = "persoane juridice"
                else:
                    continue

                # Extract table from page
                table = self.extract_table(span)
                donations = self.parse_table(table, target=party, donation_type=donation_type, source=response.url)

                # Remove duplicate entries
                for donation in donations:
                    if len(self.all_donations) == 0 or self.all_donations[-1] != donation:
                        self.all_donations.append(donation)

    def closed(self, spider):

        print("\n[donations_spider] closed() ")
        print("Unidentified Pages:\n")
        print(self.unidentified)

        dump_dataset("./data/unidentified_pages.json", self.unidentified)
        dump_dataset("./data/donations.json", self.all_donations)

        # for debug, print the names of all columns
        print_defaultdict(self.all_column_types)
