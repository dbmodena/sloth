import bz2
import csv
import math
import os
import pickle as pkl
import pymongo as pm
import re
import sys
import time
from sloth import sloth
from utils import parse_table

dataset = "wikipedia"
candidate_file = "lsh_candidates_128_8.pkl"

tab_dir_path = "datasets/" + dataset + "/"
res_dir_path = "results/" + dataset + "/"
res_cand_path_base = "res_cand.csv"
res_run_path_base = "res_run.csv"

res_cand_headers = ["id", "r_id", "r_w", "r_h", "r_a", "r_tokens", "s_id", "s_w", "s_h", "s_a", "s_tokens", "jsim"]
res_run_headers = ["cand_id", "seeds", "seed_init_time", "algo", "bw", "setup_time", "gen_cands", "gen_time",
                   "ver_cands", "ver_time", "o_num", "o_w", "o_h", "o_a", "total_time"]

run_params = [{"algo": "e", "bw": None}, {"algo": "a", "bw": 2}, {"algo": "a", "bw": 4}, {"algo": "a", "bw": 8},
              {"algo": "a", "bw": 16}, {"algo": "a", "bw": 32}]

client = pm.MongoClient()
sloth_db = client.sloth
tab_coll = sloth_db.latest_snapshot_tables


def raise_syntax_error():
    print("SyntaxError: python main.py [s|m] [r_id|first_id] [s_id|num_cand] [o|a] (-min_w) (-max_w) (-min_h) (-max_h)"
          " (-h) (-a).")
    exit(1)


def parse_arguments(args):
    """
    Parse the arguments in the form "-o val" according to the syntax defined for the table matching function
    :args[1]: the operating mode ("s" for a single candidate, "m" for a batch of candidates)
    :args[2]: the id of the table R(X) (for single mode) or the id of the first candidate to evaluate (for batch mode)
    :args[3]: the id of the table S(Y) (for single mode) or the number of candidates to evaluate (for batch mode)
    :args[4]: the largest overlaps to return ("o" only the first one, "a" all)
    :-min_w: the minimum overlap width (optional): ratio w.r.t. the smallest width if in (0.0, 1.0], effective if > 1
    :-max_w: the maximum overlap width (optional): ratio w.r.t. the smallest width if in (0.0, 1.0], effective if > 1
    :-min_h: the minimum overlap height (optional): ratio w.r.t. the smallest height if in (0.0, 1.0], effective if > 1
    :-max_h: the maximum overlap height (optional): ratio w.r.t. the smallest height if in (0.0, 1.0], effective if > 1
    :-a: the minimum overlap area (optional): ratio w.r.t. the smallest table if in (0.0, 1.0], effective if > 1
    """
    parsed_args = dict()

    # Check the number of arguments
    if len(args) not in [5, 7, 9, 11, 13, 15]:
        raise_syntax_error()

    # Parse the operating mode
    arg = args[1]
    if arg not in ["s", "m"]:
        raise_syntax_error()
    else:
        parsed_args["mode"] = arg

    # Parse the specific parameters for the operating mode
    arg = args[2]
    if re.match(r"^[0-9]+(.[0-9]+)?$", arg):
        if parsed_args["mode"] == "s":
            parsed_args["r_id"] = str(arg)
        else:
            parsed_args["first_id"] = int(arg)
    else:
        raise_syntax_error()

    arg = args[3]
    if re.match(r"^[0-9]+(.[0-9]+)?$", arg):
        if parsed_args["mode"] == "s":
            parsed_args["s_id"] = str(arg)
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

    arg_id = 5
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
        else:
            raise_syntax_error()
        arg_id += 2

    return parsed_args


def main(mode, r_id=-1, s_id=-1, first_id=-1, num_cand=-1, num_res=1, min_w=0, max_w=math.inf, min_h=0, max_h=math.inf,
         min_a=0):
    """
    Perform the task defined by the user
    :param mode: the type of task to be performed, i.e., "s" (single candidate) or "m" (batch of candidates)
    :param r_id: the id of the table R(X) (for single mode)
    :param s_id: the id of the table S(Y) (for single mode)
    :param first_id: the id of the first candidate to evaluate (for batch mode)
    :param num_cand: the number of candidates to evaluate (for batch mode)
    :param num_res: the cardinality of the result, i.e., "o" (only the first largest overlap) or "a" (all)
    :param min_w: the minimum overlap width (default 0)
    :param max_w: the maximum overlap width (default infinite)
    :param min_h: the minimum overlap height (default 0)
    :param max_h: the maximum overlap height (default infinite)
    :param min_a: the minimum overlap area (default 0)
    """
    start_time = time.time()

    # Load the candidates
    if mode == "s":
        candidates = [(r_id, s_id)]
    else:
        with bz2.BZ2File(candidate_file, "rb") as input_file:
            candidates = pkl.load(input_file)
            candidates = sorted(list(candidates), key=lambda x: (x[0], x[1]))
            input_file.close()
        print("Number of candidates: " + str(len(candidates)))

    # Write the headers in the result file describing the candidates
    if mode == "m" and not os.path.exists(res_dir_path):
        os.makedirs(res_dir_path)
    res_cand_path = res_cand_path_base.split("_")
    res_cand_path = res_cand_path[0] + "_" + str(first_id) + "_" + res_cand_path[1]
    res_cand_path = res_dir_path + res_cand_path
    if mode == "m":
        with open(res_cand_path, "w", newline="") as output_file:
            csv.writer(output_file).writerow(res_cand_headers)
            output_file.close()

    # Write the headers in the result file describing the metrics of the performed run
    res_run_path = res_run_path_base.split("_")
    res_run_path = res_run_path[0] + "_" + str(first_id) + "_" + res_run_path[1]
    res_run_path = res_dir_path + res_run_path
    if mode == "m":
        with open(res_run_path, "w", newline="") as out_file:
            csv.writer(out_file).writerow(res_run_headers)
            out_file.close()

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
        if cand[0] in tab_cache.keys():
            r_obj = tab_cache[cand[0]]
        else:
            r_obj = tab_coll.find_one({"_id": cand[0]}, {"content": 1, "num_columns": 1, "num_header_rows": 1,
                                                         "lineage": 1, "page": 1})
            tab_cache[cand[0]] = r_obj

        if cand[1] in tab_cache.keys():
            s_obj = tab_cache[cand[1]]
        else:
            s_obj = tab_coll.find_one({"_id": cand[1]}, {"content": 1, "num_columns": 1, "num_header_rows": 1,
                                                         "lineage": 1, "page": 1})
            tab_cache[cand[1]] = s_obj

        # Put the tables into a list of lists (columns) format
        r_tab = parse_table(r_obj["content"], r_obj["num_columns"], r_obj["num_header_rows"])
        s_tab = parse_table(s_obj["content"], s_obj["num_columns"], s_obj["num_header_rows"])

        # Compute the Jaccard similarity between the sets of cells of the two tables
        r_tokens = {cell for col in r_tab for cell in col}
        s_tokens = {cell for col in s_tab for cell in col}
        try:
            jsim = len(r_tokens.intersection(s_tokens)) / len(r_tokens.union(s_tokens))
        except ZeroDivisionError:
            jsim = None

        if len(r_tokens) >= 10 and len(s_tokens) >= 10:

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

            metrics = [cand_id] if mode == "m" else None

            # Detect the largest overlaps
            verbose = True if mode == "s" else False
            results, metrics = sloth(r_tab, s_tab, metrics=metrics, min_a=min_a, verbose=verbose, min_w=min_w,
                                     max_w=max_w, min_h=min_h, max_h=max_h)

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
        results = main(mode, r_id=parsed_args["r_id"], s_id=parsed_args["s_id"], num_cand=1,
                       min_a=parsed_args["delta"] if "delta" in parsed_args.keys() else 0,
                       min_w=parsed_args["min_w"] if "min_w" in parsed_args.keys() else 0,
                       max_w=parsed_args["max_w"] if "max_w" in parsed_args.keys() else math.inf,
                       min_h=parsed_args["min_h"] if "min_h" in parsed_args.keys() else 0,
                       max_h=parsed_args["max_h"] if "max_h" in parsed_args.keys() else math.inf)
    else:
        results = main(mode, first_id=parsed_args["first_id"], num_cand=parsed_args["num_cand"],
                       min_a=parsed_args["delta"] if "delta" in parsed_args.keys() else 0,
                       min_w=parsed_args["min_w"] if "min_w" in parsed_args.keys() else 0,
                       max_w=parsed_args["max_w"] if "max_w" in parsed_args.keys() else math.inf,
                       min_h=parsed_args["min_h"] if "min_h" in parsed_args.keys() else 0,
                       max_h=parsed_args["max_h"] if "max_h" in parsed_args.keys() else math.inf)
