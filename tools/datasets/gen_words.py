#!/usr/bin/env python3

import argparse
import collections
import random
import string
import sys

ALPHABET = string.ascii_lowercase


def main(filesize, num_words, word_length, words_per_line):
    words = "".join(random.choices(ALPHABET, k=num_words + word_length))
    word_distribution = collections.Counter()

    def generate_word():
        start = random.randint(0, num_words - 1)
        length = word_length
        word = words[start:start + length]
        word_distribution.update([word])
        return word

    def gen_line():
        words = [generate_word() for _ in range(words_per_line)]
        return " ".join(words)

    size = 0
    lines = 0
    while size < filesize * 1024 * 1024:
        line = gen_line()
        size += len(line) + 1
        lines += 1
        print(line)

    freq_histogram = collections.Counter(word_distribution.values())

    print(f"{lines} lines", file=sys.stderr)
    print(f"{len(word_distribution)} unique words", file=sys.stderr)
    print("Most frequent words", word_distribution.most_common(5), file=sys.stderr)

    max_freq = freq_histogram.most_common(1)[0][1]
    print("word frequency vs number of words with that frequency", file=sys.stderr)
    for freq, amount in sorted(freq_histogram.items()):
        length = int(amount / max_freq * 30)
        print(f"{freq:2} ({amount:8} times) ", "*" * length, file=sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("filesize", help="dimension of the output in MiB", type=int)
    parser.add_argument("--num-words", help="average number of unique words to generate", type=int, default=100_000)
    parser.add_argument("--word-length", help="number of characters per word", type=int, default=8)
    parser.add_argument("--words-per-line", help="average number of words per line", type=int, default=8)

    args = parser.parse_args()
    main(args.filesize, args.num_words, args.word_length, args.words_per_line)
