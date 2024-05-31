import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess


class RainfallSpider(scrapy.Spider):
    name = "rainfall_spider"
    start_urls = ["https://www.imdpune.gov.in/lrfindex.php"]

    def parse(self, response):
        dropdown_menu = response.css('select[name="subdivList"] option')

        for option in dropdown_menu:
            state_value = option.attrib['value']
            state_name = option.css('::text').get().strip()
            if state_value: 
                state_url = f"https://www.imdpune.gov.in/cmpg/subdivrainfall/{state_value}.html"
                yield scrapy.Request(state_url, callback=self.parse_state, meta={'state_name': state_name})

    def parse_state(self, response):
        state_name = response.meta['state_name']
        tables = response.css('table')

        for table in tables:
            headers = table.css('th::text').getall()
            rows = table.css('tr')
            table_data = []

            for row in rows:
                row_data = []
                cells = row.css('td')
                for cell in cells:
                    cell_text = cell.css('::text').get().strip() if cell.css('::text').get() else ""
                    row_data.append(cell_text)
                if row_data:
                    table_data.append(row_data)

            # Convert table_data to DataFrame
            df = pd.DataFrame(table_data, columns=headers)
            # Constructing the file name
            file_name = f"{state_name}_table_{tables.index(table) + 1}.csv"
            # Saving DataFrame to CSV
            df.to_csv(file_name, index=False)
            self.log(f"Saved {state_name} table {tables.index(table) + 1} data to {file_name}")

            yield {
                'state_name': state_name,
                'table_number': tables.index(table) + 1,
                'table_data': table_data
            }


def main():
    process = CrawlerProcess()
    process.crawl(RainfallSpider)
    process.start()


if __name__ == "__main__":
    main()
