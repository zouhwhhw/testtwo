import pandas as pd
import os
import argparse
from typing import Dict, Any, List, Optional, Union

class DataScreeningTool:
    """表格数据自动筛查工具"""
    
    def __init__(self):
        """初始化工具"""
        self.data = None
        self.rules = []
    
    def load_data(self, file_path: str) -> None:
        """
        加载表格数据
        
        Args:
            file_path: 文件路径
        """
        # 检查文件是否存在
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
            
        # 根据文件扩展名选择读取方式
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.csv':
            self.data = pd.read_csv(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            self.data = pd.read_excel(file_path)
        else:
            raise ValueError("不支持的文件格式，仅支持CSV和Excel文件")
            
        print(f"已加载数据，共{len(self.data)}行，{len(self.data.columns)}列")
    
    def add_rule(self, column: str, condition: str, value: Any) -> None:
        """
        添加筛查规则
        
        Args:
            column: 列名
            condition: 条件，如 '>'、'<='、'=='、'contains'、'in' 等
            value: 对比值
        """
        self.rules.append({
            'column': column,
            'condition': condition,
            'value': value
        })
        print(f"已添加规则: {column} {condition} {value}")
    
    def add_rules_from_dict(self, rules_dict: Dict[str, Dict[str, Any]]) -> None:
        """
        从字典批量添加筛查规则
        
        Args:
            rules_dict: 规则字典，格式为 {列名: {条件: 值}}
        """
        for column, conditions in rules_dict.items():
            for condition, value in conditions.items():
                self.add_rule(column, condition, value)
    
    def screen_data(self) -> pd.DataFrame:
        """
        执行数据筛查
        
        Returns:
            筛查结果DataFrame
        """
        if self.data is None:
            raise ValueError("请先加载数据")
            
        if not self.rules:
            raise ValueError("请先添加筛查规则")
            
        # 复制原始数据
        result = self.data.copy()
        
        # 应用每条筛查规则
        for rule in self.rules:
            column = rule['column']
            condition = rule['condition']
            value = rule['value']
            
            # 检查列是否存在
            if column not in result.columns:
                print(f"警告: 列 '{column}' 不存在，跳过此规则")
                continue
                
            # 根据不同条件执行筛查
            if condition == '>':
                result = result[result[column] > value]
            elif condition == '>=':
                result = result[result[column] >= value]
            elif condition == '<':
                result = result[result[column] < value]
            elif condition == '<=':
                result = result[result[column] <= value]
            elif condition == '==':
                result = result[result[column] == value]
            elif condition == '!=':
                result = result[result[column] != value]
            elif condition == 'contains':
                result = result[result[column].astype(str).str.contains(str(value), na=False)]
            elif condition == 'not contains':
                result = result[~result[column].astype(str).str.contains(str(value), na=False)]
            elif condition == 'in':
                if not isinstance(value, list):
                    value = [value]
                result = result[result[column].isin(value)]
            elif condition == 'not in':
                if not isinstance(value, list):
                    value = [value]
                result = result[~result[column].isin(value)]
            else:
                print(f"警告: 不支持的条件 '{condition}'，跳过此规则")
        
        print(f"筛查完成，符合条件的记录有{len(result)}条")
        return result
    
    def save_result(self, result: pd.DataFrame, output_path: str) -> None:
        """
        保存筛查结果
        
        Args:
            result: 筛查结果DataFrame
            output_path: 输出文件路径
        """
        # 检查输出目录是否存在，不存在则创建
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 根据文件扩展名选择保存方式
        file_ext = os.path.splitext(output_path)[1].lower()
        if file_ext == '.csv':
            result.to_csv(output_path, index=False)
        elif file_ext in ['.xlsx', '.xls']:
            result.to_excel(output_path, index=False)
        else:
            raise ValueError("不支持的输出文件格式，仅支持CSV和Excel文件")
            
        print(f"结果已保存至: {output_path}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='表格数据自动筛查工具')
    parser.add_argument('-i', '--input', required=True, help='输入文件路径')
    parser.add_argument('-o', '--output', required=True, help='输出文件路径')
    parser.add_argument('-r', '--rules', required=True, 
                        help='规则文件路径（JSON格式，例如: {"age": {"<=": 30, "in": [25, 26, 27]}, "name": {"contains": "张"}}）')
    args = parser.parse_args()
    
    try:
        # 初始化工具
        tool = DataScreeningTool()
        
        # 加载数据
        tool.load_data(args.input)
        
        # 从文件加载规则
        import json
        with open(args.rules, 'r', encoding='utf-8') as f:
            rules_dict = json.load(f)
        tool.add_rules_from_dict(rules_dict)
        
        # 执行筛查
        result = tool.screen_data()
        
        # 保存结果
        tool.save_result(result, args.output)
        
        print("数据筛查任务已完成")
        
    except Exception as e:
        print(f"执行过程中发生错误: {str(e)}")


if __name__ == "__main__":
    main()   