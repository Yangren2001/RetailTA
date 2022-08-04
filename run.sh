#!/bin/bash
echo "正在进行预处理..."
spark-submit preprocess.py
echo "预处理完成"
echo "正在进行数据分析..."
# submit spark job
spark-submit datanalysis.py
echo "数据处理完成"
echo "开始可视化，请访问9999端口查看"
# start web server
python3 web.py
