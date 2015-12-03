import random

## Distributes probability among N bins according to exponent s (0 < s < 1)
def allocate_pdist(N, s):
    ret_array = [1.0 / pow(i+1, s) for i in range(N)]
    tot = sum(ret_array)
    return map(lambda x:x/tot, ret_array)

def scramble(lst):
    for i in range(len(lst)):
        tmp = lst[i]
        rdx = random.randint(0, len(lst)-1)
        lst[i] = lst[rdx]
        lst[rdx] = tmp

def scaled_list(lst, minout, maxout):
    minin = min(lst)
    maxin = max(lst)
    return map(lambda x: (x-minin) / (maxin-minin) * (maxout - minout) + minout, lst)

def createTestDistribution(number_of_keys, max_count_per_key, min_count_per_key, exponent):
    task_counts = allocate_pdist(number_of_keys, exponent)
    scramble(task_counts)
    task_counts = scaled_list(task_counts, min_count_per_key, max_count_per_key)
    return [(i+1, int(round(t))) for (i, t) in enumerate(task_counts)]
