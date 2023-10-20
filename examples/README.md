This folders contains representative pairs of tables from Wikipedia describing typical cases where the detected largest overlap significantly differs from traditional set similarity measures, such as Jaccard similarity and overlap set similarity.

The "examples.csv" file contains the statistics about the example table pairs, reporting for each of them:
- id: the unique identifier of the table pair
- r_id (s_id): the unique identifier of the table
- r_w (s_w): the number of columns of the table
- r_h (s_h): the number of rows of the table
- r_a (s_a): the area of the table
- r_dist (s_dist): the number of distinct cell values
- seeds: the number of detected seeds
- algo: the adopted algorithm ("e" for the exact algorithm, "a" for its greedy variant)
- o_w: the number of columns of the largest overlap
- o_h: the number of rows of the largest overlap
- o_a: the area of the largest overlap
- jaccard: jaccard similarity, computed on the sets of cell values
- set_overlap: overlap set similarity, computed on the sets of cell values (normalized by the smaller set)
- sloth: area of the largest overlap normalized by the area of the smaller table
- comment: case represented by the pair

Our examples address 4 main cases:
- high_set_measures: jaccard similarity and overlap set similarity significantly greater than normalized largest overlap area (due to the repetition and the alignment of cell values)
- low_set_measures: jaccard similarity and overlap set similarity significantly smaller than normalized largest overlap area (due to the repetition and the alignment of cell values)
- low_jaccard_similarity: jaccard similarity significantly smaller than overlap set similarity and normalized largest overlap area (bias against sets of different size)
- high_set_overlap: overlap set similarity significantly greater than jaccard similarity and normalized largest overlap area

The user can explore the table pairs through the provided Jupyter notebook, simply by assigning the id of the table pair to the dedicated variable.
The notebook allows to visualize the statistics about the example table pairs from the "examples.csv" file, the two tables (with the related context, i.e., page and section in Wikipedia, and the number of declared header rows, ignored by the largest overlap computation), and the corresponding overlap, all in pandas DataFrame format.
Note that in case multiple largest overlaps were detected, for simplicity we stored the first one.

Alternatively, some examples for the 4 cases are presented in HTML format in the "html" folder.
