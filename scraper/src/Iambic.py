test_text = "On 26 May 2005 the South African Geographical Names Council (SAGNC), which is linked to the Directorate of Heritage in the Department of Arts and Culture, approved changing the name of Pretoria to Tshwane, which is already the name of the Metropolitan Municipality in which Pretoria, and a number of surrounding towns are located. Although the name change was approved by the SAGNC, it has not yet been approved by the Minister of Arts and Culture. The matter is currently under consideration while he has requested further research on the matter. Should the Minister approve the name change, the name will be published in the Government Gazette, giving the public opportunity to comment on the matter. The Minister can then refer that public response back to the SAGNC, before presenting his recommendation before parliament, who will vote on the change. Various public interest groups have warned that the name change will be challenged in court, should the minister approve the renaming. The long process involved made it unlikely the name would change anytime soon, if ever, even assuming the Minister had approved the change in early 2006."

def createStressDict(filename):
    f = open(filename, 'r')
    rdict = {}
    for line in f:
        if line[0:2] != "##": ## Comments
            word,_,rest = line.partition(" ")
            binarySyllables = [0 if "0" in x else 1 for x in rest.split("-")]
            rdict[word] = binarySyllables
    f.close()
    return rdict
