import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Параметры
num_sales = 10000  # Количество строк для raw_sales
num_customers = 1000  # Количество строк для raw_customers

# Генерация данных для raw_sales
sales_data = {
    'sale_id': range(1, num_sales + 1),
    'customer_id': np.random.randint(1, num_customers + 1, num_sales),
    'product_id': np.random.randint(1, 100, num_sales),  # Предполагаем 100 продуктов
    'store_id': np.random.randint(1, 50, num_sales),  # Предполагаем 50 магазинов
    'sale_date': [(datetime.now() - timedelta(days=np.random.randint(1, 365))).strftime('%Y-%m-%d') for _ in range(num_sales)],
    'amount': np.random.uniform(10, 1000, num_sales).round(2),
    'discount_applied': np.where(np.random.choice([True, False], num_sales, p=[0.8, 0.2]), np.random.uniform(0, 20, num_sales).round(2), None),
    'region': np.random.choice(['Europe', 'Asia', 'North America'], num_sales),
    'data_source': ['CRM_SYSTEM'] * num_sales,
    'last_updated': [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_sales)]
}
df_sales = pd.DataFrame(sales_data)
df_sales.to_csv('raw_sales.csv', index=False)

# Генерация данных для raw_customers
customers_data = {
    'customer_id': range(1, num_customers + 1),
    'first_name': [f'FirstName{i}' for i in range(1, num_customers + 1)],
    'last_name': [f'LastName{i}' for i in range(1, num_customers + 1)],
    'email': [f'customer{i}@example.com' if np.random.choice([True, False], p=[0.9, 0.1]) else None for i in range(1, num_customers + 1)],
    'registration_date': [(datetime.now() - timedelta(days=np.random.randint(1, 730))).strftime('%Y-%m-%d') for _ in range(num_customers)],
    'country': np.random.choice(['Poland', 'Germany', 'USA'], num_customers),
    'last_updated': [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_customers)]
}
df_customers = pd.DataFrame(customers_data)
df_customers.to_csv('raw_customers.csv', index=False)

print("Файлы raw_sales.csv и raw_customers.csv успешно созданы!")