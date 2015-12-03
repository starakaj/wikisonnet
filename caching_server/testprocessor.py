import random

words = ["fish", "lame", "moose", "jacks", "forest", "long", "selfish", "cormorant", "grass", "believe",
        "help", "woe", "avalanche", "simple", "pull", "frozen", "tapioca", "religion", "grout", "disk",
        "sorrel", "waste", "weasel", "concupiscent", "Mexico", "orange", "vest", "taxi", "church"]

def process(pinput):
    return ", ".join([words[random.randint(0, len(words)-1)] for _ in range(5)])
