# main_gui.py

import tkinter as tk
from tkinter import messagebox
from data_processing import process_data
import datetime

def process_files():
    try:
        # 获取文件路径
        source_order_path = entry_source_order.get()
        adjust_order_path = entry_adjust_order.get()
        name_list_path = entry_name_list.get()
        b2b_order_path = entry_b2b_order.get()

        # 确保文件路径不为空
        if not all([source_order_path, adjust_order_path, name_list_path, b2b_order_path]):
            messagebox.showerror("错误", "所有文件路径都必须提供")
            return

        # 调用数据处理函数
        process_data(source_order_path, adjust_order_path, name_list_path, b2b_order_path)

        # 获取当前时间戳
        timestamp = datetime.datetime.now().strftime("%y%m%d%H%M%S")
        output_filename = f'processed_{timestamp}.xlsx'

        messagebox.showinfo("成功", "文件处理成功！文件已保存为：" + output_filename)
    except Exception as e:
        messagebox.showerror("错误", f"发生错误：{e}")

# 创建Tkinter窗口
root = tk.Tk()
root.title("文件处理程序")

# 创建标签和输入框
tk.Label(root, text="source_order 文件路径").pack()
entry_source_order = tk.Entry(root)
entry_source_order.pack()

tk.Label(root, text="adjust_order 文件路径").pack()
entry_adjust_order = tk.Entry(root)
entry_adjust_order.pack()

tk.Label(root, text="name_list 文件路径").pack()
entry_name_list = tk.Entry(root)
entry_name_list.pack()

tk.Label(root, text="B2B_order 文件路径").pack()
entry_b2b_order = tk.Entry(root)
entry_b2b_order.pack()

# 创建处理按钮
process_button = tk.Button(root, text="处理文件", command=process_files)
process_button.pack()


# 启动GUI
root.mainloop()
