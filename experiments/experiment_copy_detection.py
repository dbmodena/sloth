import bz2
import csv
import math
import os
import pickle as pkl
import re
import sys
import time
from sloth import sloth
from utils import parse_table

dataset = "flight"  # stock | flight
mode = "clean"  # raw | clean (only for stock)
mode = mode if dataset == "stock" else "clean"

tab_dir_path_base = "datasets/" + dataset + "/" + mode + "/"
res_dir_path_intra_base = "results/" + dataset + "/single_day/" + mode + "_tables/"
res_dir_path_inter_base = "results/" + dataset + "/next_day/" + mode + "_tables/"
res_cand_path_base = "res_cand.csv"
res_run_path_base = "res_run.csv"

res_cand_headers = ["id", "r_id", "r_w", "r_h", "r_a", "r_tokens", "s_id", "s_w", "s_h", "s_a", "s_tokens", "jsim"]
res_run_headers = ["cand_id", "seeds", "seed_init_time", "algo", "bw", "setup_time", "gen_cands", "gen_time",
                   "ver_cands", "ver_time", "o_num", "o_w", "o_h", "o_a", "total_time"]

days = {"stock": ["01", "04", "05", "06", "07", "08", "11", "12", "13", "14", "15", "18", "19", "20", "21", "22", "25",
                  "26", "27", "28", "29"],
        "flight": ["2011-12-01", "2011-12-02", "2011-12-03", "2011-12-04", "2011-12-05", "2011-12-07", "2011-12-08",
                   "2011-12-09", "2011-12-10", "2011-12-11", "2011-12-12", "2011-12-13", "2011-12-14", "2011-12-15",
                   "2011-12-16", "2011-12-17", "2011-12-18", "2011-12-19", "2011-12-20", "2011-12-22", "2011-12-24",
                   "2011-12-25", "2011-12-26", "2011-12-27", "2011-12-28", "2011-12-29", "2011-12-30", "2011-12-31",
                   "2012-01-01", "2012-01-02", "2012-01-03"]}

sources = {"stock": ["advfn", "barchart", "barrons", "bloomberg", "boston-com", "bostonmerchant", "business-insider",
                     "chron", "cio-com", "cnn-money", "easystockalterts", "eresearch-fidelity-com", "finance-abc7-com",
                     "finance-abc7chicago-com", "financial-content", "finapps-forbes-com", "finviz", "fool",
                     "foxbusiness", "google-finance", "howthemarketworks", "hpcwire", "insidestocks", "investopedia",
                     "investorguide", "marketintellisearch", "marketwatch", "minyanville", "msn-money", "nasdaq-com",
                     "optimum", "paidcontent", "pc-quote", "personal-wealth-biz", "predictwallstreet", "raymond-james",
                     "renewable-energy-world", "scroli", "screamingmedia", "simple-stock-quotes", "smartmoney",
                     "stocknod", "stockpickr", "stocksmart", "stocktwits", "streetinsider-com", "thecramerreport",
                     "thestree", "tickerspy", "tmx-quotemedia", "updown", "wallstreetsurvivor", "yahoo-finance",
                     "ycharts-com", "zacks"],
           "flight": ["CO", "aa", "airtravelcenter", "allegiantair", "boston", "businesstravellogue", "den", "dfw",
                      "flightarrival", "flightaware", "flightexplorer", "flights", "flightstats", "flightview",
                      "flightwise", "flylouisville", "flytecomm", "foxbusiness", "gofox", "helloflight", "iad", "ifly",
                      "mco", "mia", "myrateplan", "mytripandmore", "orbitz", "ord", "panynj", "phl", "quicktrip", "sfo",
                      "travelocity", "ua", "usatoday", "weather", "world-flight-tracker", "wunderground"]}


def raise_syntax_error():
    print("SyntaxError: python main.py [s|m] [r_id|first_id] [s_id|num_cand] [o|a] [day] (-d) (-w) (-h) (-a).")
    exit(1)


def parse_arguments(args):
    """
    Parse the arguments in the form "-o val" according to the syntax defined for the table matching function
    :args[1]: the operating mode ("s" for a single candidate, "m" for a batch of candidates)
    :args[2]: the id of the table R(X) (for single mode) or the id of the first candidate to evaluate (for batch mode)
    :args[3]: the id of the table S(Y) (for single mode) or the number of candidates to evaluate (for batch mode)
    :args[4]: the largest overlaps to return ("o" only the first one, "a" all)
    :args[5]: the day in the dataset to analyze
    :-d: the second day in the dataset to consider in case of inter-day comparisons
    :-min_w: the minimum overlap width (optional): ratio w.r.t. the smallest width if in (0.0, 1.0], effective if > 1
    :-max_w: the maximum overlap width (optional): ratio w.r.t. the smallest width if in (0.0, 1.0], effective if > 1
    :-min_h: the minimum overlap height (optional): ratio w.r.t. the smallest height if in (0.0, 1.0], effective if > 1
    :-max_h: the maximum overlap height (optional): ratio w.r.t. the smallest height if in (0.0, 1.0], effective if > 1
    :-a: the minimum overlap area (optional): ratio w.r.t. the smallest table if in (0.0, 1.0], effective if > 1
    """
    parsed_args = dict()

    # Check the number of arguments
    if len(args) not in [6, 8, 10, 12, 14, 16, 18]:
        raise_syntax_error()

    # Parse the operating mode
    arg = args[1]
    if arg not in ["s", "m"]:
        raise_syntax_error()
    else:
        parsed_args["mode"] = arg

    # Parse the specific parameters for the operating mode
    arg = args[2]
    if re.match(r"^[0-9]+$", arg):
        if parsed_args["mode"] == "s":
            parsed_args["r_id"] = arg
        else:
            parsed_args["first_id"] = int(arg)
    else:
        raise_syntax_error()

    arg = args[3]
    if re.match(r"^[0-9]+$", arg):
        if parsed_args["mode"] == "s":
            parsed_args["s_id"] = arg
        else:
            parsed_args["num_cand"] = int(arg)
    else:
        raise_syntax_error()

    # Parse the cardinality of the result
    arg = args[4]
    if arg not in ["o", "a"]:
        raise_syntax_error()
    else:
        parsed_args["num_res"] = arg

    # Parse the day to analyze
    arg = args[5]
    if arg not in days[dataset]:
        raise_syntax_error()
    else:
        parsed_args["day"] = arg

    arg_id = 6
    while arg_id < len(args):
        arg = args[arg_id]

        if arg in ["-min_w", "-max_w", "-min_h", "-max_h", "-a"] and re.match(r"^[0-9]+(\.[0-9]+)?$", args[arg_id + 1]):
            arg_val = float(args[arg_id + 1])
            if arg_val <= 0:
                raise_syntax_error()
            elif 0 < arg_val < 1:
                parsed_args[arg.lstrip("-") if arg != "-a" else "delta"] = arg_val
            else:
                parsed_args[arg.lstrip("-") if arg != "-a" else "delta"] = int(arg_val)
        elif arg == "-d" and args[arg_id + 1] in days[dataset]:
            arg_val = args[arg_id + 1]
            parsed_args["inter_day"] = arg_val
        else:
            raise_syntax_error()
        arg_id += 2

    return parsed_args


def main(mode, r_id=-1, s_id=-1, first_id=-1, num_cand=-1, day=days[dataset][0], inter="", num_res=1, min_w=0,
         max_w=math.inf, min_h=0, max_h=math.inf, min_a=0):
    """
    Perform the task defined by the user
    :param mode: the type of task to be performed, i.e., "s" (single candidate) or "m" (batch of candidates)
    :param r_id: the id of the table R(X) (for single mode)
    :param s_id: the id of the table S(Y) (for single mode)
    :param first_id: the id of the first candidate to evaluate (for batch mode)
    :param num_cand: the number of candidates to evaluate (for batch mode)
    :param day: the day in the dataset to analyze
    :param inter: the second day in the dataset to consider in case of inter-day comparisons
    :param num_res: the cardinality of the result, i.e., "o" (only the first largest overlap) or "a" (all)
    :param min_w: the minimum overlap width (default 0)
    :param max_w: the maximum overlap width (default infinite)
    :param min_h: the minimum overlap height (default 0)
    :param max_h: the maximum overlap height (default infinite)
    :param min_a: the minimum overlap area (default 0)
    """
    start_time = time.time()
    tab_dir_path_1 = tab_dir_path_base + day + "/"
    tab_dir_path_2 = tab_dir_path_base + inter + "/"

    # Load the candidates
    if mode == "s":
        candidates = [(r_id, s_id)]
    else:
        num_tabs = len([tab for tab in os.listdir(tab_dir_path_1)])
        if inter == "":
            candidates = [(i, j) for i in range(0, num_tabs) for j in range(i + 1, num_tabs)]
        else:
            candidates = [(i, j) for i in range(0, num_tabs) for j in range(0, num_tabs)]
        print("Number of candidates: " + str(len(candidates)))

    # Write the headers in the result file describing the candidates
    res_dir_path_base = res_dir_path_intra_base if inter == "" else res_dir_path_inter_base
    res_dir_path = res_dir_path_base + day + "/"
    if mode == "m" and not os.path.exists(res_dir_path):
        os.makedirs(res_dir_path)
    res_cand_path = res_cand_path_base.split("_")
    res_cand_path = res_cand_path[0] + "_" + str(first_id) + "_" + res_cand_path[1]
    res_cand_path = res_dir_path + res_cand_path
    if mode == "m":
        with open(res_cand_path, "w", newline="") as output_file:
            csv.writer(output_file).writerow(res_cand_headers)
            output_file.close()

    # Evaluate the candidates
    results = list()  # list of the detected largest overlaps
    tab_cache = dict()  # used to store the tables whose information have already been computed
    iter_id = 0

    while iter_id < num_cand:
        cand_id = first_id + iter_id
        cand = candidates[cand_id] if mode == "m" else candidates[0]

        if mode == "m":
            if iter_id % 50 == 0:
                print(iter_id)

        # Get the information about the tables
        if inter == "" and cand[0] in tab_cache.keys():
            r_obj = tab_cache[cand[0]]
        else:
            with bz2.BZ2File(tab_dir_path_1 + str(sources[dataset][int(cand[0])]) + ".pkl", "rb") as in_file:
                tab_tuples = pkl.load(in_file)
                in_file.close()
            tab_content = [list(tup) for tup in tab_tuples]
            r_obj = {"id": cand[0], "content": tab_content, "num_columns": len(tab_content[0]), "num_header_rows": 1}
            tab_cache[cand[0]] = r_obj

        if inter == "" and cand[1] in tab_cache.keys():
            s_obj = tab_cache[cand[1]]
        else:
            tab_dir_path = tab_dir_path_1 if inter == "" else tab_dir_path_2
            with bz2.BZ2File(tab_dir_path + str(sources[dataset][int(cand[1])]) + ".pkl", "rb") as in_file:
                tab_tuples = pkl.load(in_file)
                in_file.close()
            tab_content = [list(tup) for tup in tab_tuples]
            s_obj = {"id": cand[1], "content": tab_content, "num_columns": len(tab_content[0]), "num_header_rows": 1}
            tab_cache[cand[1]] = s_obj

        # Put the tables into a list of lists (columns) format and filter out empty columns
        r_tab = parse_table(r_obj["content"], r_obj["num_columns"], r_obj["num_header_rows"])
        r_tab = [col for col in r_tab if len(set(col)) > 3]
        s_tab = parse_table(s_obj["content"], s_obj["num_columns"], s_obj["num_header_rows"])
        s_tab = [col for col in s_tab if len(set(col)) > 3]

        # Compute the Jaccard similarity between the sets of cells of the two tables
        r_tokens = {cell for col in r_tab for cell in col}
        s_tokens = {cell for col in s_tab for cell in col}
        try:
            jsim = len(r_tokens.intersection(s_tokens)) / len(r_tokens.union(s_tokens))
        except ZeroDivisionError:
            jsim = None

        # Compute the size of the two tables
        r_w = len(r_tab)  # width (i.e., number of columns) of the table R(X)
        try:
            r_h = len(r_tab[0])  # height (i.e., number of rows) of the table R(X)
        except IndexError:
            r_h = 0
        r_a = r_w * r_h  # area of the table R(X)

        s_w = len(s_tab)  # width (i.e., number of columns) of the table S(Y)
        try:
            s_h = len(s_tab[0])  # height (i.e., number of rows) of the table S(Y)
        except IndexError:
            s_h = 0
        s_a = s_w * s_h  # area of the table S(Y)

        # Save the information about the current pair in the result file describing the candidates
        if mode == "m":
            metrics = [cand_id, cand[0], r_w, r_h, r_a, len(r_tokens), cand[1], s_w, s_h, s_a, len(s_tokens), jsim]
            with open(res_cand_path, "a", newline="") as input_file:
                csv_writer = csv.writer(input_file)
                csv_writer.writerow(metrics)
                input_file.close()

        # Write the headers in the result file describing the metrics of the performed run
        res_run_path = res_run_path_base.split("_")
        res_run_path = res_run_path[0] + "_" + str(first_id) + "_" + res_run_path[1]
        res_run_path = res_dir_path + res_run_path
        if mode == "m":
            if iter_id == 0:
                with open(res_run_path, "w", newline="") as out_file:
                    csv.writer(out_file).writerow(res_run_headers)
                    out_file.close()
        metrics = [cand_id] if mode == "m" else None

        # Detect the largest overlaps
        verbose = True if mode == "s" else False
        results, metrics = sloth(r_tab, s_tab, min_a=min_a, metrics=metrics, verbose=verbose, min_w=min_w, max_w=max_w,
                                 min_h=min_h, max_h=max_h)

        if mode == "m":
            while len(metrics) < len(res_run_headers):
                metrics.append(None)
            with open(res_run_path, "a", newline="") as output_file:
                csv.writer(output_file).writerow(metrics)
                output_file.close()

        iter_id += 1

    if mode == "m":
        print("Number of performed comparisons: " + str(iter_id))
    print("Total elapsed time: " + str(time.time() - start_time) + " seconds")

    return results


if __name__ == "__main__":
    parsed_args = parse_arguments(sys.argv)
    mode = parsed_args["mode"]
    if mode == "s":
        results = main(mode, r_id=parsed_args["r_id"], s_id=parsed_args["s_id"], num_cand=1, day=parsed_args["day"],
                       inter=parsed_args["inter_day"] if "inter_day" in parsed_args.keys() else "",
                       min_a=parsed_args["delta"] if "delta" in parsed_args.keys() else 0,
                       min_w=parsed_args["min_w"] if "min_w" in parsed_args.keys() else 0,
                       max_w=parsed_args["max_w"] if "max_w" in parsed_args.keys() else math.inf,
                       min_h=parsed_args["min_h"] if "min_h" in parsed_args.keys() else 0,
                       max_h=parsed_args["max_h"] if "max_h" in parsed_args.keys() else math.inf)
    else:
        results = main(mode, first_id=parsed_args["first_id"], num_cand=parsed_args["num_cand"], day=parsed_args["day"],
                       inter=parsed_args["inter_day"] if "inter_day" in parsed_args.keys() else "",
                       min_a=parsed_args["delta"] if "delta" in parsed_args.keys() else 0,
                       min_w=parsed_args["min_w"] if "min_w" in parsed_args.keys() else 0,
                       max_w=parsed_args["max_w"] if "max_w" in parsed_args.keys() else math.inf,
                       min_h=parsed_args["min_h"] if "min_h" in parsed_args.keys() else 0,
                       max_h=parsed_args["max_h"] if "max_h" in parsed_args.keys() else math.inf)
