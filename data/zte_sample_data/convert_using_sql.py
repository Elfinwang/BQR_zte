# # import re

# # def convert_using_to_on(sql_content):
# #     """
# #     将SQL中的USING语法转换为ON语法
# #     """
# #     # 匹配 USING 子句的正则表达式
# #     # 匹配模式: ) table_alias USING (column1, column2, ...)
# #     using_pattern = r'\)\s+(\w+)\s+USING\s*\(([^)]+)\)'
    
# #     # 找到所有 USING 子句的位置和内容
# #     using_matches = list(re.finditer(using_pattern, sql_content, re.IGNORECASE))
    
# #     if not using_matches:
# #         print("未找到 USING 子句")
# #         return sql_content
    
# #     print(f"找到 {len(using_matches)} 个 USING 子句")
    
# #     # 从后往前替换，避免位置偏移影响
# #     for match in reversed(using_matches):
# #         table_alias = match.group(1)
# #         columns_str = match.group(2)
        
# #         # 解析列名列表
# #         columns = [col.strip() for col in columns_str.split(',')]
        
# #         # 构造ON条件 (使用COLLECTTIME_MO_TABLE作为左表别名，根据您的SQL结构)
# #         on_conditions = []
# #         for col in columns:
# #             on_conditions.append(f"COLLECTTIME_MO_TABLE.{col} = {table_alias}.{col}")
        
# #         on_clause = " AND ".join(on_conditions)
# #         new_join_clause = f") {table_alias} ON ({on_clause})"
        
# #         # 替换
# #         sql_content = sql_content[:match.start()] + new_join_clause + sql_content[match.end():]
    
# #     return sql_content

# # def main():
# #     # 读取SQL文件
# #     input_file = '/data1/wangyiyan/BQR/data/zte_sample_data/example_sql3_new.txt'
# #     output_file = '/data1/wangyiyan/BQR/data/zte_sample_data/example_sql3_new_converted_fixed.txt'
    
# #     try:
# #         # 读取原始SQL文件
# #         with open(input_file, 'r', encoding='utf-8') as file:
# #             sql_content = file.read()
        
# #         print(f"成功读取文件: {input_file}")
# #         print(f"文件大小: {len(sql_content)} 字符")
        
# #         # 查找所有的USING子句
# #         using_pattern = r'USING\s*\([^)]+\)'
# #         using_clauses = re.findall(using_pattern, sql_content, re.IGNORECASE)
# #         print(f"找到 {len(using_clauses)} 个 USING 子句:")
# #         for i, clause in enumerate(using_clauses):
# #             print(f"  {i+1}. {clause}")
        
# #         # 转换USING到ON
# #         converted_sql = convert_using_to_on(sql_content)
        
# #         # 保存转换后的SQL到新文件
# #         with open(output_file, 'w', encoding='utf-8') as file:
# #             file.write(converted_sql)
        
# #         print(f"\n转换完成！结果已保存到: {output_file}")
        
# #         # 验证转换结果
# #         new_using_clauses = re.findall(using_pattern, converted_sql, re.IGNORECASE)
# #         print(f"转换后剩余的 USING 子句: {len(new_using_clauses)} 个")
        
# #         if len(new_using_clauses) == 0:
# #             print("✓ 所有 USING 子句已成功转换")
# #         else:
# #             print("⚠ 仍有部分 USING 子句未转换:")
# #             for clause in new_using_clauses:
# #                 print(f"  - {clause}")
                
# #     except FileNotFoundError:
# #         print(f"错误: 找不到文件 {input_file}")
# #     except Exception as e:
# #         print(f"处理文件时出错: {str(e)}")
# #         import traceback
# #         traceback.print_exc()

# # if __name__ == "__main__":
# #     main()


# import re

# def fix_ambiguous_columns(sql_content):
#     """
#     修复SQL中的列名歧义问题
#     """
#     # 修复最外层SELECT中的歧义列名
#     # 将 SELECT collecttime 替换为 SELECT COLLECTTIME_MO_TABLE.collecttime
#     sql_content = re.sub(
#         r'SELECT\s+collecttime\s*-', 
#         'SELECT COLLECTTIME_MO_TABLE.collecttime -', 
#         sql_content,
#         flags=re.IGNORECASE
#     )
    
#     # 如果上面的替换没有生效，尝试更通用的替换
#     if 'SELECT collecttime' in sql_content and 'SELECT COLLECTTIME_MO_TABLE.collecttime' not in sql_content:
#         sql_content = re.sub(
#             r'SELECT\s+collecttime', 
#             'SELECT COLLECTTIME_MO_TABLE.collecttime', 
#             sql_content,
#             count=1,
#             flags=re.IGNORECASE
#         )
    
#     return sql_content

# def convert_using_to_on_and_fix_columns(sql_content):
#     """
#     将SQL中的USING语法转换为ON语法，并修复列名歧义
#     """
#     # 首先修复列名歧义
#     sql_content = fix_ambiguous_columns(sql_content)
    
#     # 匹配 USING 子句的正则表达式
#     using_pattern = r'\)\s+(\w+)\s+USING\s*\(([^)]+)\)'
    
#     # 找到所有 USING 子句的位置和内容
#     using_matches = list(re.finditer(using_pattern, sql_content, re.IGNORECASE))
    
#     if not using_matches:
#         print("未找到 USING 子句")
#         return sql_content
    
#     print(f"找到 {len(using_matches)} 个 USING 子句")
    
#     # 从后往前替换，避免位置偏移影响
#     for match in reversed(using_matches):
#         table_alias = match.group(1)
#         columns_str = match.group(2)
        
#         # 解析列名列表
#         columns = [col.strip() for col in columns_str.split(',')]
        
#         # 构造ON条件 (使用COLLECTTIME_MO_TABLE作为左表别名，根据您的SQL结构)
#         on_conditions = []
#         for col in columns:
#             on_conditions.append(f"COLLECTTIME_MO_TABLE.{col} = {table_alias}.{col}")
        
#         on_clause = " AND ".join(on_conditions)
#         new_join_clause = f") {table_alias} ON ({on_clause})"
        
#         # 替换
#         sql_content = sql_content[:match.start()] + new_join_clause + sql_content[match.end():]
    
#     return sql_content

# def main():
#     # 读取SQL文件
#     input_file = '/data1/wangyiyan/BQR/data/zte_sample_data/example_sql3_new.txt'
#     output_file = '/data1/wangyiyan/BQR/data/zte_sample_data/example_sql3_new_converted_fixed_final.txt'
    
#     try:
#         # 读取原始SQL文件
#         with open(input_file, 'r', encoding='utf-8') as file:
#             sql_content = file.read()
        
#         print(f"成功读取文件: {input_file}")
#         print(f"文件大小: {len(sql_content)} 字符")
        
#         # 查找所有的USING子句
#         using_pattern = r'USING\s*\([^)]+\)'
#         using_clauses = re.findall(using_pattern, sql_content, re.IGNORECASE)
#         print(f"找到 {len(using_clauses)} 个 USING 子句:")
#         for i, clause in enumerate(using_clauses):
#             print(f"  {i+1}. {clause}")
        
#         # 转换USING到ON并修复列名歧义
#         converted_sql = convert_using_to_on_and_fix_columns(sql_content)
        
#         # 保存转换后的SQL到新文件
#         with open(output_file, 'w', encoding='utf-8') as file:
#             file.write(converted_sql)
        
#         print(f"\n转换完成！结果已保存到: {output_file}")
        
#         # 验证转换结果
#         new_using_clauses = re.findall(using_pattern, converted_sql, re.IGNORECASE)
#         print(f"转换后剩余的 USING 子句: {len(new_using_clauses)} 个")
        
#         if len(new_using_clauses) == 0:
#             print("✓ 所有 USING 子句已成功转换")
#         else:
#             print("⚠ 仍有部分 USING 子句未转换:")
#             for clause in new_using_clauses:
#                 print(f"  - {clause}")
                
#         # 显示输出文件的前几行以确认内容
#         lines = converted_sql.split('\n')
#         print("\n输出文件前10行预览:")
#         for i in range(min(10, len(lines))):
#             print(f"{i+1:2d}: {lines[i]}")
                
#     except FileNotFoundError:
#         print(f"错误: 找不到文件 {input_file}")
#     except Exception as e:
#         print(f"处理文件时出错: {str(e)}")
#         import traceback
#         traceback.print_exc()

# if __name__ == "__main__":
#     main()




import re

def fix_all_ambiguous_columns(sql_content):
    """
    修复SQL中的所有列名歧义问题
    """
    # 修复最外层SELECT中的歧义列名
    sql_content = re.sub(
        r'SELECT COLLECTTIME_MO_TABLE\.collecttime - INTERVAL \'60 MINUTE\' AS begintime, collecttime AS endtime', 
        'SELECT COLLECTTIME_MO_TABLE.collecttime - INTERVAL \'60 MINUTE\' AS begintime, COLLECTTIME_MO_TABLE.collecttime AS endtime', 
        sql_content,
        flags=re.IGNORECASE
    )
    
    # 修复内层SELECT中的歧义列名 (第4行)
    # 找到内层SELECT并修复collecttime引用
    lines = sql_content.split('\n')
    
    # 查找FROM子查询中的SELECT语句
    in_inner_select = False
    for i, line in enumerate(lines):
        # 查找内层SELECT的开始
        if 'FROM (' in line and 'SELECT collecttime' in '\n'.join(lines[i:i+5]):
            in_inner_select = True
            continue
            
        # 在内层SELECT中查找collecttime引用
        if in_inner_select and 'SELECT collecttime' in line:
            # 修复这一行
            lines[i] = line.replace('SELECT collecttime', 'SELECT COLLECTTIME_MO_TABLE.collecttime')
            
        # 查找内层SELECT结束
        if in_inner_select and line.strip() == ') COLLECTTIME_MO_TABLE':
            in_inner_select = False
            
    # 重新组合SQL
    sql_content = '\n'.join(lines)
    
    return sql_content

def main():
    # 读取SQL文件
    input_file = '/data1/wangyiyan/BQR/data/zte_sample_data/example_sql3_new_converted_fixed_final.txt'
    output_file = '/data1/wangyiyan/BQR/data/zte_sample_data/example_sql3_new_fully_fixed.txt'
    
    try:
        # 读取原始SQL文件
        with open(input_file, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        print(f"成功读取文件: {input_file}")
        print(f"文件大小: {len(sql_content)} 字符")
        
        # 修复所有列名歧义
        fixed_sql = fix_all_ambiguous_columns(sql_content)
        
        # 保存修复后的SQL到新文件
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(fixed_sql)
        
        print(f"\n修复完成！结果已保存到: {output_file}")
        
        # 显示输出文件的前15行以确认内容
        lines = fixed_sql.split('\n')
        print("\n输出文件前15行预览:")
        for i in range(min(15, len(lines))):
            print(f"{i+1:2d}: {lines[i]}")
                
    except FileNotFoundError:
        print(f"错误: 找不到文件 {input_file}")
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()