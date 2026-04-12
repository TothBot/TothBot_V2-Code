"""
DocDCN:     1011001
DocTitle:   TothBot_Package_Entry
DocVersion: dv1_7
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/__main__.py
DocDate:    04-12-2026
DocTime:    23:59:59 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_7   04-12-2026  DC header added per 0311001 v1_1, 0311004 v1_1,
                      1011001 dv1_7. Package entry point.
                      Governed by 1011001 Engineering_Best_Practices dv1_7.

============================================================

TothBot V2 package entry point. Invokes startup_sequence.run().
============================================================
"""
from tothbot.startup_sequence import run; run()