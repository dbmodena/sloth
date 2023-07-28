import bz2
import os
import pandas as pd
import pickle as pkl

raw_stock_dir_path = "raw_stock/"
raw_stock_tab_dir_path = "tables/stock/raw/"
clean_stock_dir_path = "clean_stock/"
clean_stock_tab_dir_path = "tables/stock/clean/"
clean_flight_dir_path = "clean_flight/"
clean_flight_tab_dir_path = "tables/flight/clean/"

stock_sources = ["advfn", "barchart", "barrons", "bloomberg", "boston-com", "bostonmerchant", "business-insider",
                 "chron", "cio-com", "cnn-money", "easystockalterts", "eresearch-fidelity-com", "finance-abc7-com",
                 "finance-abc7chicago-com", "financial-content", "finapps-forbes-com", "finviz", "fool", "foxbusiness",
                 "google-finance", "howthemarketworks", "hpcwire", "insidestocks", "investopedia", "investorguide",
                 "marketintellisearch", "marketwatch", "minyanville", "msn-money", "nasdaq-com", "optimum",
                 "paidcontent", "pc-quote", "personal-wealth-biz", "predictwallstreet", "raymond-james",
                 "renewable-energy-world", "scroli", "screamingmedia", "simple-stock-quotes", "smartmoney", "stocknod",
                 "stockpickr", "stocksmart", "stocktwits", "streetinsider-com", "thecramerreport", "thestree",
                 "tickerspy", "tmx-quotemedia", "updown", "wallstreetsurvivor", "yahoo-finance", "ycharts-com", "zacks"]
flight_sources = ["CO", "aa", "airtravelcenter", "allegiantair", "boston", "businesstravellogue", "den", "dfw",
                  "flightarrival", "flightaware", "flightexplorer", "flights", "flightstats", "flightview",
                  "flightwise", "flylouisville", "flytecomm", "foxbusiness", "gofox", "helloflight", "iad", "ifly",
                  "mco", "mia", "myrateplan", "mytripandmore", "orbitz", "ord", "panynj", "phl", "quicktrip", "sfo",
                  "travelocity", "ua", "usatoday", "weather", "world-flight-tracker", "wunderground"]
nasdaq_cols = ["Symbol", "Change %", "Change $", "Last Sale", "1y Target Est:", "Market Value of Listed Security",
               "Shares Outstanding", "P/E Ratio", "Earnings Per Share", "Share Volume", "Forward P/E (1yr)",
               "Today's High / Low", "52 Wk High / Low", "Ex Dividend Date", "Annualized Dividend", "Previous Close",
               "Dividend Payment Date", "Current Yield", "Beta", "Community Sentiment", "Best Bid / Ask",
               "50 Day Avg. Daily Volume", "Date of NASDAQ Official Open Price:", "NASDAQ Official Open Price:",
               "Date of NASDAQ Official Close Price:", "NASDAQ Official Close Price:", "Special Dividend",
               "Special Dividend Date", "Special Dividend Payment Date"]
clean_stock_headers = ["Source", "Symbol", "Change %", "Last trading price", "Open price", "Change $", "Volume",
                       "Today's high", "Today's low", "Previous close", "52wk High", "52wk Low", "Shares Outstanding",
                       "P/E", "Market cap", "Yield", "Dividend", "EPS"]
clean_flight_headers = ["Source", "Flight#", "Scheduled departure", "Actual departure", "Departure gate",
                        "Scheduled arrival", "Actual arrival", "Arrival gate"]


def parse_raw_stock_tables():
    daily_dirs = [d for d in os.walk(raw_stock_dir_path)][0][1]
    for src in stock_sources:
        for d in daily_dirs:
            # create the output daily directory
            out_path = raw_stock_tab_dir_path + d + "/"
            if not os.path.exists(out_path):
                os.makedirs(out_path)
            # parse the raw file
            raw_src_file_path = raw_stock_dir_path + d + "/raw/" + src + ".txt"
            src_file_path = out_path + src + ".pkl"
            with open(raw_src_file_path, "r") as in_file:
                lines = in_file.readlines()
            tab_cont = list()
            stock_data = dict()
            num_lines = len(lines)
            for i in range(0, num_lines):
                if lines[i] != "\n":
                    if ":=" in lines[i]:
                        if "&nbsp;" in lines[i]:
                            lines[i] = lines[i].replace("&nbsp;", " ")
                        if "&rsquo;" in lines[i]:
                            lines[i] = lines[i].replace("&rsquo;", "'")
                        if "&ndash;" in lines[i]:
                            lines[i] = lines[i].replace("&ndash;", "-")
                        line_items = lines[i].split(":=")
                        stock_data[line_items[0]] = line_items[1].rstrip("\n").strip()
                        for j in range(1, 4):
                            if i < num_lines - j and ":=" not in lines[i + j] and lines[i + j] != "\n":
                                if stock_data[line_items[0]] != "":
                                    stock_data[line_items[0]] += " "
                                if "&nbsp;" in lines[i + j]:
                                    lines[i + j] = lines[i + j].replace("&nbsp;", " ")
                                stock_data[line_items[0]] += lines[i + j].rstrip("\n").strip()
                            else:
                                break
                    else:
                        if "Symbol" not in stock_data.keys():
                            stock_data["Symbol"] = lines[i].lstrip("(").rstrip(")\n")
                else:
                    if i < num_lines - 1 and (":=" in lines[i + 1] or not lines[i + 1].startswith("(")):
                        pass
                    else:
                        if src == "nasdaq-com":
                            labels = {k: v for k, v in stock_data.items() if k.startswith("Label")}
                            stock_data = {k: v for k, v in stock_data.items() if not k.startswith("Label")}
                            for lab in labels:
                                lab_id = lab.split("@")[1]
                                if labels[lab] == "null":
                                    del stock_data["Value@" + lab_id]
                                else:
                                    val = stock_data["Value@" + lab_id]
                                    del stock_data["Value@" + lab_id]
                                    stock_data[labels[lab]] = val
                            for col in nasdaq_cols:
                                if col not in stock_data:
                                    stock_data[col] = "null"
                        tab_cont.append(stock_data)
                        stock_data = dict()
            if not os.path.exists(src_file_path):
                tab_df = pd.DataFrame.from_records(tab_cont)
                header = [col for col in tab_df.columns]
                tab_tup = [tuple(header)] + list(tab_df.itertuples(index=False, name=None))
                with bz2.open(src_file_path, "wb") as out_file:
                    pkl.dump(tab_tup, out_file)
                    out_file.close()


def parse_clean_stock_tables():
    daily_files = [f for f in os.walk(clean_stock_dir_path)][0][2]
    for f in daily_files:
        day = f.split("-")[-1].rstrip(".txt")
        # create the output daily directory
        out_dir_path = clean_stock_tab_dir_path + day + "/"
        if not os.path.exists(out_dir_path):
            os.makedirs(out_dir_path)
        # parse the clean file
        with open(clean_stock_dir_path + f, "r") as in_file:
            lines = in_file.readlines()
            in_file.close()
        parsed_lines = list()
        for line in lines:
            parsed_lines.append(tuple(line.split("\t")[:-1]))
        ds_clean = pd.DataFrame.from_records(parsed_lines, columns=clean_stock_headers)
        for source in stock_sources:
            source_ds = ds_clean[ds_clean["Source"] == source].drop(columns=["Source"])
            source_lines = [tuple(clean_stock_headers[1:])] + list(source_ds.itertuples(index=False, name=None))
            with bz2.open(out_dir_path + "/" + source + ".pkl", "wb") as out_file:
                pkl.dump(source_lines, out_file)
                out_file.close()


def parse_clean_flight_tables():
    daily_files = [f for f in os.walk(clean_flight_dir_path)][0][2]
    for f in daily_files:
        day = f.split("-d")[0]
        # create the output daily directory
        out_dir_path = clean_flight_tab_dir_path + day + "/"
        if not os.path.exists(out_dir_path):
            os.makedirs(out_dir_path)
        # parse the clean file
        with open(clean_flight_dir_path + f, "r") as in_file:
            lines = in_file.readlines()
            in_file.close()
        parsed_lines = list()
        for line in lines:
            parsed_lines.append(tuple([a.replace("\n", " ").strip() for a in line.split("\t")]))
        ds_clean = pd.DataFrame.from_records(parsed_lines, columns=clean_flight_headers)
        for source in flight_sources:
            source_ds = ds_clean[ds_clean["Source"] == source].drop(columns=["Source"])
            source_lines = [tuple(clean_flight_headers[1:])] + list(source_ds.itertuples(index=False, name=None))
            with bz2.open(out_dir_path + "/" + source + ".pkl", "wb") as out_file:
                pkl.dump(source_lines, out_file)
                out_file.close()


if __name__ == "__main__":
    parse_raw_stock_tables()
    parse_clean_stock_tables()
    parse_clean_flight_tables()
