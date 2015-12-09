from __future__ import print_function
import os
import ascii
import urllib
from pyfiglet import figlet_format

tc = "  "

def printPoem(title, poem_lines, imageURL=None):
    if imageURL is not None:
        ext = imageURL.split('.')[-1]
        urllib.urlretrieve(imageURL, '/tmp/img.' + ext)
        img_as_text = ascii.covertImageToAscii('/tmp/img.'+ext, 80, 0.43, False)

    with open('/tmp/poem.txt', 'w') as f:
        if imageURL is not None:
            for row in img_as_text:
                print(row, file=f)
        # banner = figlet_format(title, font='alphabet').split('\n')
        # for l in banner:
        #     print(l, file=f)
        print(title, file=f)
        print("\n", file=f)
        for i,line in enumerate(poem_lines):
            if i >= len(poem_lines)-2:
                print("\t" + tc + line, file=f)
            else:
                print("\t" + line, file=f)
        print("\n", file=f)
        print(("Wikisonnet is an automatic poem algorithm by\n"
        "Ana Giraldo-Wingler, "
        "Cassie Tarakajian, "
        "and Sam Tarakajian,"
        "\npowered by all 26,932,623+ contributors to en/Wikipedia."), file=f)
    os.system("lpr -P Tally_Dascom_1125 /tmp/poem.txt")