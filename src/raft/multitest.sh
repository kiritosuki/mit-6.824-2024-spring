#!/bin/bash

# 配置项
TEST_RUNS=100
TEST_NAME="." # 如果你想跑全量，可以改成 "."
LOG_DIR="/Users/kirito/mysrc/logtxt"
INFO_FILE="$LOG_DIR/info.txt"
ERROR_FILE="$LOG_DIR/error.txt"

# 初始化文件
> $INFO_FILE
> $ERROR_FILE

echo "🚀 开始 Raft $TEST_NAME 压力测试，共 $TEST_RUNS 次..."
echo "📂 日志目录: $LOG_DIR"

for ((i=1; i<=TEST_RUNS; i++))
do
    echo -n "第 $i 次测试中... "
    
    # 运行测试，捕获所有输出到临时文件
    # 使用 -race 检查竞态，这是过夜测试的标配
    # 注意：请确保你在 raft 源码目录下运行此脚本，或者修改下方的 go test 路径
    go test -v -run $TEST_NAME > tmp_log.txt 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ PASSED"
        echo "==== Run $i PASSED ====" >> $INFO_FILE
        cat tmp_log.txt >> $INFO_FILE
        echo -e "\n" >> $INFO_FILE
    else
        echo "❌ FAILED! 详情已记录至 error.txt"
        echo "==== Run $i FAILED ====" >> $ERROR_FILE
        cat tmp_log.txt >> $ERROR_FILE
        echo -e "\n" >> $ERROR_FILE
        # 如果你想在第一次失败时就停止，可以取消下面这一行的注释
        # exit 1
    fi
done

# 清理临时文件
rm tmp_log.txt

echo "🎉 测试完成！请检查 $LOG_DIR 下的日志文件。"
