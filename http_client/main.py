#! /usr/bin/env python3

import json
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk

import requests


def send_request():
    server = server_entry.get().strip()
    uri = uri_entry.get().strip()
    method = method_var.get()
    token = token_entry.get().strip()
    json_body = json_text.get(1.0, tk.END).strip()

    if not server or not uri:
        messagebox.showwarning("Ошибка", "Введите адрес сервера и URI!")
        return

    url = f"{server.rstrip('/')}/{uri.lstrip('/')}"

    headers = {}
    data = None

    if token:
        headers["Authorization"] = f"Bearer {token}"

    if json_body:
        try:
            data = json.loads(json_body)
            headers["Content-Type"] = "application/json"
        except json.JSONDecodeError as e:
            messagebox.showerror("Ошибка JSON", f"Некорректный JSON:\n{e}")
            return

    try:
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, json=data, headers=headers)
        elif method == "PUT":
            response = requests.put(url, json=data, headers=headers)
        elif method == "DELETE":
            response = requests.delete(url, json=data, headers=headers)
        else:
            response_text.delete(1.0, tk.END)
            response_text.insert(tk.END, f"Метод {method} не поддерживается")
            return

        response_text.delete(1.0, tk.END)

        response_text.insert(tk.END, f"Status: {response.status_code}\n")
        response_text.insert(
            tk.END, f"Content-Type: {response.headers.get('Content-Type')}\n"
        )
        response_text.insert(
            tk.END, f"Content-Length: {response.headers.get('Content-Length')}\n\n"
        )

        response_text.insert(tk.END, "BODY as text:\n")
        response_text.insert(tk.END, response.text)

        response_text.insert(tk.END, "\n\nBODY as hex:\n")
        response_text.insert(tk.END, response.content.hex(" "))

    except Exception as e:
        response_text.delete(1.0, tk.END)
        response_text.insert(tk.END, f"Ошибка запроса: {e}")


root = tk.Tk()
root.title("Простой HTTP-клиент")

tk.Label(root, text="HTTP-сервер:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
server_entry = tk.Entry(root, width=50)
server_entry.grid(row=0, column=1, padx=5, pady=5)
server_entry.insert(0, "http://localhost:8000")

tk.Label(root, text="HTTP-метод:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
method_var = tk.StringVar(value="GET")
method_combo = ttk.Combobox(
    root,
    textvariable=method_var,
    values=["GET", "POST", "PUT", "DELETE"],
    state="readonly",
)
method_combo.grid(row=1, column=1, padx=5, pady=5)

tk.Label(root, text="URI:").grid(row=2, column=0, sticky="e", padx=5, pady=5)
uri_entry = tk.Entry(root, width=50)
uri_entry.grid(row=2, column=1, padx=5, pady=5)
uri_entry.insert(0, "/")

tk.Label(root, text="Authorization token:").grid(
    row=3, column=0, sticky="e", padx=5, pady=5
)
token_entry = tk.Entry(root, width=50, show="*")
token_entry.grid(row=3, column=1, padx=5, pady=5)

tk.Label(root, text="JSON тело запроса:").grid(
    row=4, column=0, sticky="ne", padx=5, pady=5
)
json_text = scrolledtext.ScrolledText(root, width=50, height=8)
json_text.grid(row=4, column=1, padx=5, pady=5)
json_text.insert(tk.END, '{\n  "user_id": "ABC123"\n}')

send_button = tk.Button(root, text="Отправить запрос", command=send_request)
send_button.grid(row=5, column=0, columnspan=2, pady=10)

response_text = scrolledtext.ScrolledText(root, width=70, height=20)
response_text.grid(row=6, column=0, columnspan=2, padx=5, pady=5)

root.mainloop()
