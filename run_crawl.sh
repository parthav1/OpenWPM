#!/bin/bash

cd /root/OpenWPM
source /root/miniconda3/etc/profile.d/conda.sh
conda activate openwpm

/root/miniconda3/envs/openwpm/bin/python /root/OpenWPM/run_parallel.py /root/OpenWPM/iffys.txt /root/OpenWPM/newsguard_untrustworthy.txt /root/OpenWPM/newsguard_trustworthy.txt

conda deactivate
conda activate openwpmcrawlanalysis
/root/miniconda3/envs/openwpmcrawlanalysis/bin/python /root/CrawlDataExtractionPipeline/extraction/fast_extract_tables.py
