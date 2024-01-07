import pandas as pd
from river import linear_model, metrics, stream

# Chargement du dataset CSV (par exemple, 'example_data.csv')
dataset_path = 'dataset_GOOGL.csv'
data = pd.read_csv(dataset_path)
data = list(data.to_records(index=True))


# Initialisation du modèle de régression linéaire en ligne
model = linear_model.LinearRegression()

# Initialisation des métriques pour évaluer le modèle
mae = metrics.MAE()

# Boucle d'apprentissage en ligne
for (x, y) in enumerate(data):
    #print(y)
    target = y['price']
    features = [y['sp500'], y['cac40'], y['nikkei']]
    # # Extraction des caractéristiques (features) et de la cible
    # features = row[['sp500', 'cac40', 'nikkei']]
    # target = row['price']

    # # Apprentissage en ligne
    model.learn_one(features, target)

    # # Prédiction en ligne
    prediction = model.predict_one(features)

    # # Mise à jour des métriques
    mae.update(target, prediction)

# Affichage de la métrique MAE
print(f"Mean Absolute Error (MAE): {mae.get():.4f}")


# Récupérer les indices des colonnes de prix
# prix_col = data[0].index('prix')
# prix_stock_col_indices = [data[0].index(f'{stock}_prix') for stock in ['GOOGL', 'META', 'AMZN']]

# # Récupérer les valeurs des colonnes de prix
# prix = data[0][prix_col]
# prix_stock = [data[0][index] for index in prix_stock_col_indices]

# print(f"Prix : {prix}")
# print(f"Prix des stocks : {prix_stock}")
