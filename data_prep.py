import pandas as pd
from datetime import datetime


class column_and_year_extractor:
    def __init__(
        self,
        path,
        year,
        columns=["Incident Date", "Total Number of Dead and Missing", "Coordinates"],
    ):
        self.path = path
        self.columns = columns
        self.year = year
        self.data = pd.read_csv(path)

    def extract(self):
        df = self.data[self.columns]
        df = df.rename(
            columns={
                "Incident Date": "Reported Date",
                "Total Number of Dead and Missing": "Total Dead and Missing",
                "Coordinates": "Location Coordinates",
            }
        )

        df["Reported Date"] = df["Reported Date"].apply(self.convert_date)

        # df = df[df["Reported Date"].dt.year == self.year]

        df.to_csv(f"data_prepared/MissingMigrants_{self.year}.csv", index=False)

    def convert_date(self, date_str):
        date_obj = datetime.strptime(date_str, "%a, %m/%d/%Y - %H:%M")
        return date_obj.strftime("%B %d, %Y")


# data_2020 = column_and_year_extractor(
#    "data_raw/MissingMigrants-Global-2023-12-07--16_48_56.csv", 2020
# )
# data_2020.extract()

data_2021 = column_and_year_extractor(
    "data_raw/MissingMigrants-Global-2023-12-07--16_48_56.csv", 2021
)
data_2021.extract()

# data_2022 = column_and_year_extractor(
#    "data_raw/MissingMigrants-Global-2023-12-07--16_49_29.csv", 2022
# )
# data_2022.extract()

data_2023 = column_and_year_extractor(
    "data_raw/MissingMigrants-Global-2023-12-07--16_49_29.csv", 2023
)
data_2023.extract()


def concatenate_data(paths):
    concatenated_df = pd.DataFrame()
    for path in paths:
        data = pd.read_csv(path)
        concatenated_df = pd.concat([concatenated_df, data], ignore_index=True)

    return concatenated_df


paths = [
    "https://gist.githubusercontent.com/curran/a9656d711a8ad31d812b8f9963ac441c/raw/MissingMigrants-Global-2019-10-08T09-47-14-subset.csv",
    "https://gist.githubusercontent.com/Tofriev/f9caa19b33010ba79fe8fec2971beb12/raw/MissingMigrants_2021.csv",
    "https://gist.githubusercontent.com/Tofriev/06374b71092d100909e89364ac8984be/raw/MissingMigrants_2023.csv",
]

combined_df = concatenate_data(paths)
combined_df.to_csv("data_prepared/MissingMigrants_2019_2021_2023.csv", index=False)
