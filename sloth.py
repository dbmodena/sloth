import bitarray as ba
import itertools as it
import math
import pandas as pd
import time
import timeout_decorator
import variables as var
from utils import to_bag_counter


@timeout_decorator.timeout(var.timeout_s)
def detect_seeds(r_tab, s_tab, r_w, s_w, min_h):
    """
    Detect the seeds (i.e., the single-column-mappings whose area is greater than zero)
    :param r_tab: the table R(X), in a list of lists (columns) format
    :param s_tab: the table S(Y), in a list of lists (columns) format
    :param r_w: the width (i.e., number of columns) of the table R(X)
    :param s_w: the width (i.e., number of columns) of the table S(Y)
    :param min_h: the minimum overlap height
    """
    seeds = list()  # list of the seeds (a.k.a. seed list)
    t_w = r_w + s_w  # total number of columns of the two tables

    # Transform the columns into bags of cells
    # r_bags = [to_bag([col]) for col in r_tab]  # list of the bags of cells of the columns from the table R(X)
    r_bags = [to_bag_counter([col]) for col in r_tab]
    # s_bags = [to_bag([col]) for col in s_tab]  # list of the bags of cells of the columns from the table S(Y)
    s_bags = [to_bag_counter([col]) for col in s_tab]

    # Detect the seeds
    t_m = t_w * ba.bitarray('0')  # total mapping: track all the columns appearing in the seeds
    for x in range(0, r_w):
        for y in range(0, s_w):
            # o = r_bags[x].intersection(s_bags[y])  # overlap of the columns x and y
            o = r_bags[x] & s_bags[y]
            # h = len(o)  # height of the overlap
            h = sum(o.values())
            if h > max(0, min_h - 1):
                s = (x, y)  # human-readable representation of the seed as the tuple (x, y)
                m = t_w * ba.bitarray('0')  # mapping: bit array tracking the columns appearing in the mapping
                m[x] = True  # track the column x in the mapping
                m[r_w + y] = True  # track the column y in the mapping
                t_m |= m  # update the total mapping with the columns of the current seed
                seeds.append((s, m, h))  # the seed is register as a tuple of 3 elements: ((x, y), mapping, height)

    # Sort the seeds by increasing dominance (tie-break: column distance, x, y)
    seeds.sort(key=lambda seed: (1 / seed[2], abs(seed[0][0] - seed[0][1]), seed[0][0], seed[0][1]), reverse=False)

    # Compute the width of the combinations in the top lattice level
    top_lev = min(t_m.count(True, 0, r_w), t_m.count(True, r_w))

    return seeds, top_lev


def compute_bag_intersection(r_tab, s_tab, seed_ids, seeds):
    """
    Compute the intersection of the bags of tuples determined by the given seed ids
    (refer to the function detect_largest_overlaps for the parameter descriptions)
    """
    col_pairs = [seeds[i][0] for i in seed_ids]
    col_pairs.sort(key=lambda s: s[0], reverse=False)
    # r_bag = to_bag([r_tab[i] for i in [c_p[0] for c_p in col_pairs]])
    r_bag = to_bag_counter([r_tab[i] for i in [c_p[0] for c_p in col_pairs]])
    # s_bag = to_bag([s_tab[i] for i in [c_p[1] for c_p in col_pairs]])
    s_bag = to_bag_counter([s_tab[i] for i in [c_p[1] for c_p in col_pairs]])

    # return list(r_bag.intersection(s_bag))
    return r_bag & s_bag


@timeout_decorator.timeout(var.timeout_a)
def approximate_algorithm(bw, r_tab, s_tab, seeds, num_seeds, top_lev, theta, min_w, max_w, min_h, max_h, results,
                          res_h, complete, verbose, metrics):
    """
    Beam-search-based approximate algorithm to detect the largest overlaps between the two tables R(X) and S(Y)
    (refer to the function detect_largest_overlaps for the parameter descriptions)
    - the seed is represented as a tuple ((col_pair), bit_array, h)
    - the probe is represented as a tuple ([seed_ids], bit_array, h, [comp_seed_ids])
    """
    start_time = time.time()
    if metrics is not None:
        metrics = metrics[:-3]
        metrics.append("a")
        metrics.append(bw)

    setup_time = 0.0
    if metrics is not None:
        metrics.append(setup_time)

    gen_cands = 0
    ver_cands = 0
    ver_time = 0.0

    # If multiple seeds were detected (otherwise, already done), perform lattice traversal through beam search
    start_gen_time = time.time()
    if num_seeds > 1:
        s_comp = [i for i in range(0, num_seeds)]  # list of the indices of the compatible seeds (updated for each node)
        probes = [([i], seeds[i][1], seeds[i][2], s_comp)
                  for i in range(0, min(bw, num_seeds))]  # bw seeds guiding the beam search
        lev_w = 2  # move to the upper levels (i.e., combinations of lev_w seeds)
        top_lev = min(top_lev, max_w)  # top level of the lattice to be considered

        while len(probes) > 0 and lev_w <= top_lev:

            req_h = max(math.ceil(theta / top_lev), min_h)  # minimum height required to produce results (top level)
            stop_id = None  # index of the first seed to be ignored for its height
            for i in range(0, num_seeds):
                if seeds[i][2] < req_h:
                    stop_id = i
                    break

            lev_cache = dict()  # dict of the verified candidates for the current level (to avoid repetitions)
            s_comps = list()  # list of the compatible seeds for the next level for every probe

            # For every probe, generate and verify all its candidates
            p_ctr = -1  # probe counter used to track for every candidate the probe(s) that generated it
            for p in probes:
                p_ctr += 1
                p_s, p_m, p_h, p_c = p

                if stop_id is not None:
                    p_c = [c for c in p_c if c < stop_id]  # filter out the seeds which cannot produce results
                s_inc = set()  # set of the incompatible seeds to be ignored in the next level for the current probe

                # Iterate on every compatible seeds to generate and verify its produced candidate
                for i in p_c:

                    # Check that the seed is compatible
                    if (p_m & seeds[i][1]).count(True) > 0:
                        s_inc.add(i)

                    else:
                        c_s = p_s + [i]  # add the seed to the list of seeds of the current candidate

                        gen_cands += 1

                        # Check if the current candidate has already been verified
                        c_hash = hash(tuple(sorted(c_s)))

                        if c_hash not in lev_cache.keys():
                            start_ver_time = time.time()

                            # c_h = len(compute_bag_intersection(r_tab, s_tab, tuple(c_s), seeds))
                            c_h = sum(compute_bag_intersection(r_tab, s_tab, tuple(c_s), seeds).values())

                            ver_cands += 1

                            c_m = p_m | seeds[i][1]  # update the bit array for the current candidate

                            # Insert into the level cache
                            lev_cache[c_hash] = (c_s, c_m, c_h, [p_ctr])

                            end_ver_time = time.time()
                            ver_time += (end_ver_time - start_ver_time)

                        else:
                            lev_cache[c_hash][3].append(p_ctr)

                        # Check if the candidate fits the required height
                        if lev_cache[c_hash][2] < req_h:
                            s_inc.add(i)

                s_comps.append(set(p_c).difference(s_inc))

            # Update theta and the results
            cands = sorted([v for k, v in lev_cache.items() if v[2] >= req_h],
                           key=lambda c: c[2], reverse=True)  # auto tie-break: generation order (i.e., seed relevance)
            num_cands = len(cands)
            if num_cands == 0:
                break

            # Update the results (if needed)
            if lev_w >= min_w:
                use_cands = [c for c in cands if c[2] <= max_h]
                if len(use_cands) > 0:
                    max_c_h = use_cands[0][2]
                    max_c_a = lev_w * max_c_h
                    theta = max(theta, max_c_a)
                    if theta == max_c_a:
                        results = [tuple(c[0])
                                   for c in use_cands if c[2] == max_c_h] if complete else [tuple(use_cands[0][0])]
                        res_h = max_c_h

            # Select the probes for the next level
            i = 0
            probes = list()
            while len(probes) < min(bw, num_cands) and i < num_cands:
                if cands[i][2] >= req_h:
                    g_c = cands[i][3]  # probes that generated the considered candidate
                    n_c = s_comps[g_c[0]]  # compatible seeds for the current candidate in the next level
                    if len(g_c) > 1:
                        for g in g_c[1:]:
                            n_c = n_c.intersection(s_comps[g])
                    if len(n_c) > 0:
                        probes.append((cands[i][0], cands[i][1], cands[i][2], n_c))
                i += 1

            lev_w += 1

    gen_time = (time.time() - start_gen_time) - ver_time

    tot_time = time.time() - start_time

    num_res = len(results)
    res_w = len(results[0]) if num_res > 0 else 0
    res_a = res_w * res_h

    if verbose:
        print("Generated " + str(gen_cands) + " candidate(s) in " + str(gen_time) + " seconds.")
        print("Verified " + str(ver_cands) + " candidate(s) in " + str(ver_time) + " seconds.")
        print("Detected " + str(num_res) + " largest overlap(s) in " + str(tot_time) + " seconds.")
        if num_res > 0:
            print("Size of the detected largest overlap(s): " + str(res_w) + " columns, " + str(res_h) +
                  " rows, " + str(res_a) + " cells.")
    if metrics is not None:
        to_app = [gen_cands, gen_time, ver_cands, ver_time, num_res, res_w, res_h, res_a]
        for a in to_app:
            metrics.append(a)

    return results, metrics


def exact_algorithm_setup(seeds, top_lev, theta, min_w, max_w):
    """
    Initialize the generator priority queue, representing the levels of the lattice
    (refer to the function detect_largest_overlaps for the parameter descriptions)
    - the lattice level is represented as a tuple (w, seed_ptr, max_a)
    """
    gen_pq = list()

    lev_w = max(2, min_w)  # start from the combinations of 2 seeds (seeds alone are already considered in the result)
    while lev_w <= min(top_lev, max_w):
        s_ptr = lev_w - 1  # position of the seed with the max height that can be dominant in the level
        lev_max_a = lev_w * seeds[s_ptr][2]  # maximum area that can be reached by the level
        if lev_max_a >= theta:
            gen_pq.append((lev_w, s_ptr, lev_max_a))  # store the level as the tuple (lev_w, s_ptr, lev_max_a)
        lev_w += 1  # move to the upper lattice level

    gen_pq.sort(key=lambda lev: (lev[2], lev[0]), reverse=True)

    return gen_pq


@timeout_decorator.timeout(var.timeout_e)
def exact_algorithm(r_tab, s_tab, r_w, s_w, seeds, num_seeds, top_lev, theta, min_w, max_w, min_h, max_h, results,
                    res_h, complete, verbose, metrics):
    """
    Exact algorithm to detect the largest overlaps between the two tables R(X) and S(Y)
    (refer to the main function for the description of the parameters)
    - the seed is represented as a tuple (col_pair, bit_array, h)
    - the candidate is represented as a tuple (seed_ids, seed_set, w, max_h, max_a, verified)
    - the lattice level is represented as a tuple (w, seed_ptr, max_a)
    - the cached verified candidate is represented as a tuple (seed_set, h)
    """
    start_time = time.time()
    if metrics is not None:
        metrics.append("e")
        metrics.append(None)

    # Setup: initialize the two priority queues (i.e., generator and candidates)
    start_setup_time = time.time()
    gen_pq = exact_algorithm_setup(seeds, top_lev, theta, min_w, max_w)
    cand_pq = list()
    setup_time = time.time() - start_setup_time
    if metrics is not None:
        metrics.append(setup_time)

    gen_cands = 0
    ver_cands = 0
    gen_time = 0.0
    ver_time = 0.0

    cache = list()  # used to store the actual heights for the verified mappings

    # Iterate on the two priority queues to detect the largest overlap
    while len(cand_pq) > 0 or len(gen_pq) > 0:

        # Generate the candidates (if needed)
        while len(gen_pq) > 0:

            # Initialize the bounds for the two priority queues
            top_g = gen_pq[0]
            gen_b = top_g[2]  # max area of the top item
            cand_b = cand_pq[0][4] if len(cand_pq) > 0 else 0  # max area of the top item
            if cand_b >= gen_b:
                break

            # Generate the new candidates
            start_gen_time = time.time()
            s_ptd_id = top_g[1]
            s_ptd = seeds[s_ptd_id]
            s_prc = [s for s in seeds[:s_ptd_id]]
            c_w = top_g[0]

            # If the pointed seed can generate valid candidates, compute all its combinations
            m = (r_w + s_w) * ba.bitarray('0')
            m |= s_ptd[1]
            for s in s_prc:
                m |= s[1]

            if min(c_w, m.count(True, 0, r_w), m.count(True, r_w)) >= c_w:
                new_cands = [comb + (s_ptd_id,) for comb in it.combinations(range(0, len(s_prc)), c_w - 1)]
                gen_cands += len(new_cands)

                # For each candidate, check if it can be inserted into the candidate priority queue
                for nc in new_cands:

                    # Check if the new candidate contains conflicting seeds
                    c_m = (r_w + s_w) * ba.bitarray('0')
                    for i in nc:
                        c_m |= seeds[i][1]

                    if c_m.count(True, 0, r_w) == c_m.count(True, r_w) == c_w:

                        # Correct the new candidate max height based on the already verified subsets
                        c_s = set(nc)
                        c_max_h = s_ptd[2]
                        for v_m in cache:
                            if len(v_m[0].intersection(c_s)) == len(v_m[0]):
                                c_max_h = min(c_max_h, v_m[1])

                        # Insert the new candidate into the candidate priority queue
                        c_max_a = c_w * c_max_h
                        if c_max_a >= theta and c_max_h >= min_h:
                            # the candidate is represented as a tuple (seed_ids, seed_set, w, max_h, max_a, verified)
                            cand_pq.append((nc, c_s, c_w, c_max_h, c_max_a, False))
                            cand_pq.sort(key=lambda cand: (cand[4], cand[2]), reverse=True)

            # Update the level
            ud_s_ptr = top_g[1] + 1
            if ud_s_ptr < num_seeds:
                ud_lev_a = top_g[0] * seeds[ud_s_ptr][2]
                if ud_lev_a < theta:
                    del gen_pq[0]
                else:
                    gen_pq[0] = (top_g[0], ud_s_ptr, ud_lev_a)
                    gen_pq.sort(key=lambda lev: (lev[2], lev[0]), reverse=True)
            else:
                del gen_pq[0]

            gen_time += (time.time() - start_gen_time)

        # Check the candidates
        if len(cand_pq) > 0:
            start_ver_time = time.time()
            top_c = cand_pq[0]

            # If the top candidate has already been verified, check for its insertion in the result set
            if top_c[5]:
                if len(results) > 0:  # if the result set is not empty
                    if top_c[2] > len(results[0]):  # if it is larger than the current results, remove them
                        results = [top_c[0]]  # probably you could directly prevent the verification of these candidates
                        res_h = top_c[3]
                    elif top_c[2] == len(results[0]):  # if it has the same width as the current results, append it
                        results.append(top_c[0])
                else:
                    results = [top_c[0]]
                    res_h = top_c[3]
                del cand_pq[0]

                # If only the first largest overlap has to be returned, terminate
                if not complete:
                    break

            else:

                # Compute the overlap (as the intersection of the bags of tuples)
                ver_cands += 1
                c_h = sum(compute_bag_intersection(r_tab, s_tab, top_c[0], seeds).values())
                c_a = top_c[2] * c_h
                cache.append((top_c[1], c_h))
                if c_a < theta or c_h < min_h or c_h > max_h:
                    del cand_pq[0]
                else:
                    cand_pq[0] = (top_c[0], top_c[1], top_c[2], c_h, c_a, True)
                    theta = c_a

                # Update the candidates (update the height of the supersets and use theta to prune)
                to_del = list()
                for i in range(0, len(cand_pq)):
                    top_c_s, top_c_w = top_c[1], top_c[2]
                    if cand_pq[i][2] > top_c_w:
                        if len(top_c_s.intersection(cand_pq[i][1])) == top_c_w:
                            if c_h < cand_pq[i][3]:
                                cand_pq[i] = (cand_pq[i][0], cand_pq[i][1], cand_pq[i][2], c_h, cand_pq[i][2] * c_h,
                                              cand_pq[i][5])
                    if cand_pq[i][4] < theta or cand_pq[i][3] < min_h:
                        to_del.append(i)
                to_del.reverse()
                for d in to_del:
                    del cand_pq[d]
                cand_pq.sort(key=lambda cand: (cand[4], cand[2]), reverse=True)

                # Update the levels (use theta to prune)
                to_del = list()
                for i in range(0, len(gen_pq)):
                    if gen_pq[i][2] < theta:
                        to_del.append(i)
                to_del.reverse()
                for d in to_del:
                    del gen_pq[d]

            ver_time += (time.time() - start_ver_time)

    end_time = time.time()
    tot_time = end_time - start_time

    num_res = len(results)
    res_w = len(results[0]) if num_res > 0 else 0
    res_a = res_w * res_h

    if verbose:
        print("Generated " + str(gen_cands) + " candidate(s) in " + str(gen_time) + " seconds.")
        print("Verified " + str(ver_cands) + " candidate(s) in " + str(ver_time) + " seconds.")
        print("Detected " + str(num_res) + " largest overlap(s) in " + str(tot_time) + " seconds.")
        if num_res > 0:
            print("Size of the detected largest overlap(s): " + str(res_w) + " columns, " + str(res_h) +
                  " rows, " + str(res_a) + " cells.")
    if metrics is not None:
        to_app = [gen_cands, gen_time, ver_cands, ver_time, num_res, res_w, res_h, res_a]
        for a in to_app:
            metrics.append(a)

    return results, metrics


def sloth(r_tab, s_tab, min_a=0, min_w=0, max_w=math.inf, min_h=0, max_h=math.inf, bw=var.default_bw, complete=False,
          verbose=True, metrics=None):
    """
    Detect the largest overlaps between the two tables R(X) and S(Y)
    :param r_tab: the table R(X), in a list of lists (columns) format
    :param s_tab: the table S(Y), in a list of lists (columns) format
    :param min_a: the minimum overlap area: ratio w.r.t. the smallest table if in (0.0, 1.0], effective if > 1
    :param min_w: the minimum overlap width: ratio w.r.t. the smallest width if in (0.0, 1.0], effective if > 1
    :param max_w: the maximum overlap width: ratio w.r.t. the smallest width if in (0.0, 1.0], effective if > 1
    :param min_h: the minimum overlap height: ratio w.r.t. the smallest height if in (0.0, 1.0], effective if > 1
    :param max_h: the maximum overlap height: ratio w.r.t. the smallest height if in (0.0, 1.0], effective if > 1
    :param bw: the beam width parameter for the greedy approximation
    :param complete: if set to True, detect all largest overlaps; otherwise, stop after the first one is detected
    (guarantees only for the area)
    :param verbose: if set to True, print information about the advances in the detection process
    :param metrics: the list to store the achieved metrics (if not None)
    """
    start_time = time.time()
    results = list()  # list of the detected largest overlaps
    res_h = 0  # height of the detected largest overlaps

    # Compute the size of the two tables
    r_w = len(r_tab)  # width (i.e., number of columns) of the table R(X)
    r_h = len(r_tab[0]) if r_w > 0 else 0  # height (i.e., number of rows) of the table R(X)
    r_a = r_w * r_h  # area of the table R(X)
    s_w = len(s_tab)  # width (i.e., number of columns) of the table S(Y)
    s_h = len(s_tab[0]) if s_w > 0 else 0  # height (i.e., number of rows) of the table S(Y)
    s_a = s_w * s_h  # area of the table S(Y)

    # Compute the bounds for the overlap (width, height, area)
    min_a = int(min_a * min(r_a, s_a)) if 0 < min_a <= 1 else max(int(min_a), 0)
    min_h = int(min_h * min(r_h, s_h)) if 0 < min_h <= 1 else max(int(min_h), 0)
    max_h = int(max_h * min(r_h, s_h)) if 0 < max_h <= 1 else max(int(max_h) if max_h < math.inf else max_h, 0)
    min_w = int(min_w * min(r_w, s_w)) if 0 < min_w <= 1 else max(int(min_w), 0)
    max_w = int(max_w * min(r_w, s_w)) if 0 < max_w <= 1 else max(int(max_w) if max_w < math.inf else max_w, 0)
    if min_a > min(r_a, s_a) or min_h > min(min(r_h, s_h), max_h) or min_w > min(min(r_w, s_w), max_w):
        if verbose:
            print("No largest overlap has been detected.")
            print("Total elapsed time: " + str(time.time() - start_time) + " seconds.")
        return results, metrics

    # Detect the seeds
    start_seed_init_time = time.time()

    try:
        seeds, top_lev = detect_seeds(r_tab, s_tab, r_w, s_w, min_h)
        num_seeds = len(seeds)
    except Exception as exc:
        seeds = list()
        num_seeds = -1
        top_lev = 0
        print(exc)

    seed_init_time = time.time() - start_seed_init_time
    if verbose:
        print("Detected " + str(num_seeds) + " seed(s) in " + str(seed_init_time) + " seconds.")
    if metrics is not None:
        metrics.append(num_seeds)
        metrics.append(seed_init_time)

    if num_seeds <= 0:
        if verbose:
            print("No largest overlap has been detected.")
            print("Total elapsed time: " + str(time.time() - start_time) + " seconds.")
        return results, metrics

    # Compute the required minimum overlap area (theta)
    theta = min_a  # dynamic threshold to prune useless candidates
    if min_w <= 1:
        use_seed_ids = [i for i in range(0, len(seeds)) if seeds[i][2] <= max_h]
        if len(use_seed_ids) > 0:
            max_s_a = seeds[use_seed_ids[0]][2]  # maximum area of the seeds
            theta = max(theta, max_s_a)
            if theta == max_s_a:
                # list of the (temporary) detected largest overlaps (store the seed indices)
                results = [(i,) for i in use_seed_ids if seeds[i][2] == max_s_a] if complete else [(use_seed_ids[0],)]
                res_h = max_s_a

    # Detect the largest overlaps
    try:
        results, metrics = exact_algorithm(r_tab, s_tab, r_w, s_w, seeds, num_seeds, top_lev, theta, min_w, max_w,
                                           min_h, max_h, results, res_h, complete, verbose, metrics)
    except Exception as exc:
        print(exc)
        if var.run_approximate:
            try:
                results, metrics = approximate_algorithm(bw, r_tab, s_tab, seeds, num_seeds, top_lev, theta, min_w,
                                                         max_w, min_h, max_h, results, res_h, complete, verbose,
                                                         metrics)
            except Exception as exc:
                print(exc)

    tot_time = time.time() - start_time
    if verbose:
        print("Total elapsed time: " + str(tot_time) + " seconds.")
    if metrics is not None:
        metrics.append(tot_time)

    results = [([seeds[s_id][0] for s_id in res],
                list(compute_bag_intersection(r_tab, s_tab, res, seeds).elements())) for res in results]  # [(m, o)...]
    if verbose:
        if len(results) > 0:
            print(pd.DataFrame.from_records(results[0][1]).sort_values(by=0, axis="index", ignore_index=True))

    return results, metrics
