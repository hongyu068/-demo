#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
健壮的风控系统完整流程执行器
包含严格的返回码检查、依赖管理、日志收集和错误处理
"""

import os
import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path
import json

# 设置日志
def setup_logging():
    """设置日志系统"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

# 管道步骤定义
PIPELINE_STEPS = [
    {
        "name": "环境检查",
        "command": "python setup_environment.py",
        "required_files": [],
        "output_files": [],
        "critical": True
    },
    {
        "name": "数据接入测试",
        "command": "python sql/data_ingestion/test_connection.py",
        "required_files": [],
        "output_files": [],
        "critical": False
    },
    {
        "name": "数据清洗",
        "command": "python pyspark/data_cleaning/clean_data.py",
        "required_files": ["data/sample_tx_log.csv"],
        "output_files": ["data/cleaned_tx_log.csv", "data/risk_fact_table.csv"],
        "critical": True
    },
    {
        "name": "特征工程",
        "command": "python pyspark/feature_engineering/extract_features.py",
        "required_files": ["data/risk_fact_table.csv"],
        "output_files": ["data/features.csv", "data/benford_expected.csv"],
        "critical": True
    },
    {
        "name": "Benford检测",
        "command": "python pyspark/benford_detection/benford_analysis.py",
        "required_files": ["data/features.csv", "data/benford_expected.csv"],
        "output_files": ["data/benford_results.csv", "data/benford_risk_scores.csv"],
        "critical": True
    },
    {
        "name": "行为聚类",
        "command": "python pyspark/clustering/behavior_clustering.py",
        "required_files": ["data/features.csv"],
        "output_files": ["data/clustering_results.csv"],
        "critical": True
    },
    {
        "name": "模型验证",
        "command": "python pyspark/validation/model_validation.py",
        "required_files": ["data/clustering_results.csv", "data/benford_results.csv"],
        "output_files": ["data/validation_results.csv"],
        "critical": True
    },
    {
        "name": "创建仪表盘",
        "command": "python dashboard/create_dashboard.py",
        "required_files": ["data/clustering_results.csv", "data/benford_results.csv"],
        "output_files": ["dashboard/interactive_dashboard.html"],
        "critical": False
    },
    {
        "name": "生成报告",
        "command": "python reports/generate_report.py",
        "required_files": ["data/clustering_results.csv", "data/benford_results.csv"],
        "output_files": ["reports/risk_monitoring_report.md"],
        "critical": False
    }
]

class PipelineExecutor:
    """管道执行器"""
    
    def __init__(self, logger):
        self.logger = logger
        self.results = []
        self.failed_steps = []
        
    def check_file_exists(self, file_path):
        """检查文件是否存在"""
        return Path(file_path).exists()
    
    def check_prerequisites(self, step):
        """检查步骤前置条件"""
        missing_files = []
        
        for required_file in step["required_files"]:
            if not self.check_file_exists(required_file):
                missing_files.append(required_file)
        
        if missing_files:
            self.logger.error(f"步骤 '{step['name']}' 缺少必需文件: {missing_files}")
            return False
        
        return True
    
    def execute_step(self, step):
        """执行单个步骤"""
        step_name = step["name"]
        command = step["command"]
        
        self.logger.info(f"开始执行: {step_name}")
        self.logger.info(f"命令: {command}")
        
        # 检查前置条件
        if not self.check_prerequisites(step):
            if step["critical"]:
                raise RuntimeError(f"关键步骤 '{step_name}' 前置条件不满足")
            else:
                self.logger.warning(f"跳过步骤 '{step_name}' - 前置条件不满足")
                return {"name": step_name, "status": "skipped", "message": "前置条件不满足"}
        
        # 执行命令
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=1800  # 30分钟超时
            )
            
            # 记录输出
            if result.stdout:
                self.logger.info(f"标准输出:\n{result.stdout}")
            if result.stderr:
                self.logger.warning(f"错误输出:\n{result.stderr}")
            
            # 检查返回码
            if result.returncode != 0:
                error_msg = f"步骤 '{step_name}' 执行失败，返回码: {result.returncode}"
                self.logger.error(error_msg)
                
                if step["critical"]:
                    raise RuntimeError(error_msg)
                else:
                    self.failed_steps.append(step_name)
                    return {"name": step_name, "status": "failed", "message": error_msg}
            
            # 检查输出文件
            missing_outputs = []
            for output_file in step["output_files"]:
                if not self.check_file_exists(output_file):
                    missing_outputs.append(output_file)
            
            if missing_outputs:
                warning_msg = f"步骤 '{step_name}' 完成但缺少输出文件: {missing_outputs}"
                self.logger.warning(warning_msg)
                
                if step["critical"]:
                    raise RuntimeError(warning_msg)
            
            self.logger.info(f"步骤 '{step_name}' 执行成功")
            return {"name": step_name, "status": "success", "message": "执行成功"}
            
        except subprocess.TimeoutExpired:
            error_msg = f"步骤 '{step_name}' 执行超时"
            self.logger.error(error_msg)
            
            if step["critical"]:
                raise RuntimeError(error_msg)
            else:
                self.failed_steps.append(step_name)
                return {"name": step_name, "status": "timeout", "message": error_msg}
        
        except Exception as e:
            error_msg = f"步骤 '{step_name}' 执行异常: {e}"
            self.logger.error(error_msg)
            
            if step["critical"]:
                raise RuntimeError(error_msg)
            else:
                self.failed_steps.append(step_name)
                return {"name": step_name, "status": "error", "message": error_msg}
    
    def execute_pipeline(self):
        """执行完整管道"""
        self.logger.info("=== 开始执行风控系统完整流程 ===")
        start_time = datetime.now()
        
        try:
            for i, step in enumerate(PIPELINE_STEPS, 1):
                self.logger.info(f"进度: {i}/{len(PIPELINE_STEPS)}")
                
                result = self.execute_step(step)
                self.results.append(result)
                
                # 如果是关键步骤失败，立即停止
                if result["status"] in ["failed", "error", "timeout"] and step["critical"]:
                    raise RuntimeError(f"关键步骤失败: {result['message']}")
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            # 生成执行报告
            self.generate_execution_report(start_time, end_time, duration)
            
            # 统计结果
            success_count = sum(1 for r in self.results if r["status"] == "success")
            total_count = len(self.results)
            
            self.logger.info(f"=== 管道执行完成 ===")
            self.logger.info(f"成功步骤: {success_count}/{total_count}")
            self.logger.info(f"失败步骤: {len(self.failed_steps)}")
            self.logger.info(f"总耗时: {duration}")
            
            if self.failed_steps:
                self.logger.warning(f"失败的步骤: {', '.join(self.failed_steps)}")
            
            return success_count == total_count
            
        except Exception as e:
            self.logger.error(f"管道执行失败: {e}")
            return False
    
    def generate_execution_report(self, start_time, end_time, duration):
        """生成执行报告"""
        report = {
            "execution_time": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "duration": str(duration)
            },
            "steps": self.results,
            "summary": {
                "total_steps": len(self.results),
                "success_steps": sum(1 for r in self.results if r["status"] == "success"),
                "failed_steps": len(self.failed_steps),
                "success_rate": sum(1 for r in self.results if r["status"] == "success") / len(self.results) * 100
            }
        }
        
        # 保存报告
        report_file = Path("logs") / f"execution_report_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"执行报告已保存: {report_file}")

def main():
    """主函数"""
    # 设置日志
    logger = setup_logging()
    
    # 创建执行器
    executor = PipelineExecutor(logger)
    
    # 执行管道
    success = executor.execute_pipeline()
    
    # 退出码
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 