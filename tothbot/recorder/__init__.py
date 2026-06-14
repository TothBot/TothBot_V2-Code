"""recorder - logging and the two-stream trade record (mod:Logger).

Skeleton anchor; populated alongside the data layer (S2+). Houses, per the
0500000 dv1_240 organism decomposition (section 7):
  mod:Logger                              - async logging to tothbot.log via
                                            channel:logger_async_queue (AR-014)
  contract:Two_Stream_Record_Architecture - the trade-record write path
  alerting                                - CRITICAL escalation + SMTP alert to
                                            alerts@tothbot.com (HR-LG-007/009)

Named `recorder` (not `logging`) to avoid shadowing the stdlib logging module.

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
