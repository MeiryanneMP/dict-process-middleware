import pandas as pd
import json
import os
import re


def read_csv(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    df = pd.read_csv(file_path, delimiter=";")

    print("Colunas encontradas:", df.columns.tolist())
    return df


def process_csv_data(df):
    winners = []

    column_mapping = {
        'year': ['year', 'Ano', 'ano'],
        'producers': ['producers', 'name', 'nomes', 'studio'],
        'winner': ['winner', 'vencedor', 'vencedor_ano', 'won']
    }

    def map_column(column):
        for col in column_mapping[column]:
            if col in df.columns:
                return col
        return None

    year_col = map_column('year')
    producer_col = map_column('producers')
    winner_col = map_column('winner')

    if not year_col:
        print("Aviso: A coluna 'year' não foi encontrada!")
    if not producer_col:
        print("Aviso: A coluna 'producers' não foi encontrada!")
    if not winner_col:
        print("Aviso: A coluna 'winner' não foi encontrada!")

    if not (year_col and producer_col and winner_col):
        return []

    df[year_col] = df[year_col].apply(
        lambda x: str(x).strip() if pd.notnull(x) else '')
    df[producer_col] = df[producer_col].apply(
        lambda x: str(x).strip() if pd.notnull(x) else '')
    df[winner_col] = df[winner_col].apply(
        lambda x: str(x).strip() if pd.notnull(x) else '')

    df = df[(df[year_col] != '') & (
        df[producer_col] != '') & (df[winner_col] != '')]

    df_winners = df[
        (df[winner_col].str.lower() == 'yes') |
        (df[producer_col].str.lower().apply(
            lambda x: df[winner_col].str.contains(x, case=False).any()))
    ]

    print(df_winners[[year_col, producer_col, winner_col]])

    for _, row in df_winners.iterrows():
        producers = row[producer_col]
        year = row[year_col]

        producer_list = []
        for producer in producers.split(','):
            producer = producer.strip()

            if 'and' in producer:
                producer_list.extend([p.strip()
                                     for p in re.split(r'\s+and\s+', producer)])
            else:
                producer_list.append(producer)

        for producer in producer_list:
            winners.append({
                "producer": producer,
                "year": int(year) if year else None
            })

    return winners


def main():
    file_path = 'data/movielist.csv'
    df = read_csv(file_path)
    winners_data = process_csv_data(df)
    print(json.dumps(winners_data, indent=2))
    with open('winners.json', 'w', encoding='utf-8') as json_file:
        json.dump(winners_data, json_file, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
