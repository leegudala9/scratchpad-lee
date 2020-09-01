from Levenshtein import distance


def full_distance(stra, strb):
    if type(stra) != str or type(strb) != str:
        return 0
    wordsa = stra.split(" ")
    wordsb = strb.split(" ")

    if len(wordsa) > 1 or len(wordsb) > 1:
        common_words = set(wordsa).intersection(set(wordsb))
        common_len = len(common_words)
        if common_len > 0:
            mx = max(len(wordsa), len(wordsb))
            return common_len / mx

    return 1 - (distance(stra, strb) / max(len(stra), len(strb)))
