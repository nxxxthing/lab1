import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor as Executor
from functools import reduce


def merge(dict1, dict2):
    for key in dict2:
        if key in dict1:
            dict1[key] = dict1[key] + dict2[key]
        else:
            dict1[key] = dict2[key]
    return dict1


def count_words_in_file(path):
    result = dict()

    with open(path, 'r') as f:
        lines = f.read().splitlines()

        for line in lines:
            text = line.split()
            for word in text:
                if word in result.keys():
                    result[word] += 1
                else:
                    result[word] = 1

    return result


def get_file_names(path_names):
    result = []
    for path in path_names:
        for f_name in os.listdir(path):
            if f_name[-4:] != '.txt':
                continue
            result.append(os.path.join(path, f_name))

    return result


def work_with_directories(path_names):
    files = get_file_names(path_names)

    result = dict()
    for file in files:
        result = merge(result, count_words_in_file(file))

    print(result)


def work_with_directories_map_reduce(path_names):
    files = get_file_names(path_names)
    file_word_counters = map(count_words_in_file, files)
    total_word_counter = reduce(merge, list(file_word_counters))

    print(total_word_counter)


def work_with_directories_threads(path_names):
    files = get_file_names(path_names)
    file_word_counters = Executor().map(count_words_in_file, files)
    total_word_counter = reduce(merge, list(file_word_counters))

    print(total_word_counter)


def main(path_names):
    if len(path_names) < 2:
        print('Wrong arguments!')
        return

    type = path_names[0]
    if type == 'normal':
        func = work_with_directories
    elif type == 'map_reduce':
        func = work_with_directories_map_reduce
    elif type == 'threads':
        func = work_with_directories_threads
    else:
        print('Wrong type!')
        return

    time_start = time.time()
    func(path_names[1:])
    time_end = time.time()

    print("Time elapsed:", round(time_end - time_start, 5), 'sec')


if __name__ == '__main__':
    main(sys.argv[1:])
