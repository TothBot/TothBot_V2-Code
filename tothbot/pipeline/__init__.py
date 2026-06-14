"""pipeline - the TothBot trading pipeline (gates) and selection control.

Skeleton anchor; populated in sessions S3-S4. Houses, per the 0500000
dv1_240 organism decomposition (sections 3, 7):
  mod:Signal_Pipeline      - the G1-G8 gate chain (entry candidate to order)
  mod:Selection_Controller - G5 selection; cooldown + consecutive-loss state
  Gate-7 Risk Guard        - coder-detail sub-figure (Diagram 8 of 10)
  Gate-8 Position Sizer    - coder-detail sub-figure (Diagram 9 of 10);
                             enforces acceptance rule A1 (net 1:1.5 R:R floor)

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
