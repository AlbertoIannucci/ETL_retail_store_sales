from modello_base import ModelloBase
import pandas as pd
import pymysql

class DatasetCleaner(ModelloBase):

     def __init__(self, dataset_path):
         self.dataframe = pd.read_csv(dataset_path)
         self.dataframe_sistemato = self.sistemazione()

     # Metodo di sistemazione del dataframe
     def sistemazione(self):
         # Copia del dataframe
         df_sistemato = self.dataframe.copy()
         # Suddivisione variabili con nan
         variabili_categoriali = ["Item", "Discount Applied"]
         variabili_quantitative = ["Price Per Unit", "Quantity", "Total Spent"]
         # Sostituzione nan per categoriali
         for col in df_sistemato.columns:
             if col in variabili_categoriali:
                 df_sistemato[col] = df_sistemato[col].fillna(df_sistemato[col].mode()[0])
         # Sostituzione nan per quantitative
         for col in df_sistemato.columns:
             if col in variabili_quantitative:
                 df_sistemato[col] = df_sistemato[col].fillna(df_sistemato[col].median())
         # Conversione Transaction Date
         df_sistemato["Transaction Date"] = pd.to_datetime(df_sistemato["Transaction Date"])
         # Conversion Quantity
         df_sistemato["Quantity"] = df_sistemato["Quantity"].astype(int)
         # Rimappatura etichette
         df_sistemato = df_sistemato.rename(columns={
             "Transaction ID":"id_transaction",
             "Customer ID":"id_customer",
             "Category":"category",
             "Item":"item",
             "Price Per Unit":"price_per_unit",
             "Quantity":"quantity",
             "Total Spent":"total_spent",
             "Payment Method":"payment_method",
             "Location":"location",
             "Transaction Date":"transaction_date",
             "Discount Applied":"discount_applied"
         })

         return df_sistemato

# Funzione per stabilire una connesione con il database
def getconnection():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="retail_store"
    )

# Funzione per creare la tabella retail_store_sales
def creazione_tabella():
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                sql = ("CREATE TABLE IF NOT EXISTS retail_store_sales("
                       "id_transaction VARCHAR(30) PRIMARY KEY,"
                       "id_customer VARCHAR(30) NOT NULL,"
                       "category VARCHAR(30) NOT NULL,"
                       "item VARCHAR(30) NOT NULL,"
                       "price_per_unit FLOAT NOT NULL,"
                       "quantity INT NOT NULL,"
                       "total_spent FLOAT NOT NULL,"
                       "payment_method VARCHAR(30) NOT NULL,"
                       "location VARCHAR(30) NOT NULL,"
                       "transaction_date DATE NOT NULL,"
                       "discount_applied BOOLEAN NOT NULL"
                       ");")
                cursor.execute(sql)
                connection.commit()
                return cursor.rowcount
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None

# Funzione per caricare i dati nel database (senza intestazione)
def load_dati_db(df_sistemato):
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                for index, row in df_sistemato.iterrows():
                    sql = ("INSERT INTO retail_store_sales(id_transaction, id_customer, category, item, price_per_unit,"
                           " quantity, total_spent, payment_method, location, transaction_date, discount_applied)"
                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);")

                    cursor.execute(sql, (
                        row["id_transaction"],
                        row["id_customer"],
                        row["category"],
                        row["item"],
                        row["price_per_unit"],
                        row["quantity"],
                        row["total_spent"],
                        row["payment_method"],
                        row["location"],
                        row["transaction_date"],
                        row["discount_applied"]
                    ))

                connection.commit()
                print("Dati esportati con successo.")
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None

modello = DatasetCleaner("../Dataset/dataset.csv")
# Passo 1. Analisi generali del dataset
#modello.analisi_generali(modello.dataframe)
# Risultati:
# Osservazioni= 12575; Variabili= 11; Tipi= object e float64; Valori nan= presenti
# Passo 2. Analisi dei valori univoci
#modello.analisi_valori_univoci(modello.dataframe, ["Transaction ID", "Transaction Date"])
# Valori nan camuffati: non presenti
# Passo 3. Suddivisione delle variabili con valori nan in base alla tipologia (categoriali e quantitative)
# Passo 4. Strategia valori nan per variabili categoriali
# Non eseguo il drop in quanto andrei a perdere più del 10% (deve essere < 5%)
# Eseguo la sostituzione con la moda
# Passo 5. Analisi degli outliers
# modello.individuazione_outliers(modello.dataframe_sistemato, ["Transaction ID", "Customer ID",
#                                                               "Category", "Item", "Payment Method", "Location",
#                                                               "Transaction Date", "Discount Applied"])
# Risultati:
# Price Per Unit= 0%
# Quantity= 0%
# Total Spent= 0.47%
# Passo 6. Strategia valori nan per varibaili quantitative
# Non eseguo il drop in quanto andrei a perdere più del 10% (deve essere < 5%)
# Eseguo la sostituzione con la mediana
# Passo 7. Analisi degli outliers
# modello.individuazione_outliers(modello.dataframe_sistemato, ["Transaction ID", "Customer ID",
#                                                               "Category", "Item", "Payment Method", "Location",
#                                                               "Transaction Date", "Discount Applied"])
# Risultati:
# Price Per Unit= 0%
# Quantity= 0%
# Total Spent= 1.2%
# Total Spent cresciuto (0.47% -> 1.2%). Percentuale tollerabile. (Outliers inferiore al 10/15%)
# Passo 8. Conversione Transaction Date da object a datetime
# Passo 9. Conversion Quantity da float a int
# Passo 10. Rimappatura etichette
# Passo 11. Analisi generali
#modello.analisi_generali(modello.dataframe_sistemato)
# Risultati:
# Osservazioni= 12575; Variabili= 10; Tipi= objcet, int, float, datetime e boll; Valori nan= nessuno
# Passo 12. Stabilisco la connesione con il database
# Passo 13. Creo la tabella retail_store_sales
#creazione_tabella()
# Passo 14. Load dei dati
#load_dati_db(modello.dataframe_sistemato)