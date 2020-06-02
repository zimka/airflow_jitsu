## Насколько похорошела Москва

```python


    from collections import Counter
    import sys
    import requests


    def main(api_key):
        """
        Prints 5 streets in Moscow with the highest number of free wifi points
        """
        creds = {'api_key': api_key}
        base_url = "https://apidata.mos.ru/v1/datasets/"
        datasets = requests.post(
            'https://apidata.mos.ru/v1/datasets',
            params=creds,
            json=['Id', 'CategoryId', 'Caption']
        ).json()

        wifi_datasets = [
            d for d in datasets
            if d['CategoryId'] == 121 and 'wi-fi' in d['Caption'].lower()
        ]

        points = []
        for wfd in wifi_datasets:
            response = requests.get(
                base_url + f"{wfd['Id']}/rows",
                params=creds
            )
            assert response.ok
            points.extend(response.json())

        valid_points = points
        streets = []
        for vp in valid_points:
            if not 'Location' in vp['Cells']:
                # no street for parks wifi
                continue
            street = vp['Cells']['Location'].split(',')[1]
            for _ in range(vp['Cells']['NumberOfAccessPoints']):
                streets.append(street)

        print("Street, points")
        for k, v in Counter(streets).most_common(5):
            print(k, v)


    if __name__ == "__main__":
        if len(sys.argv) != 2:
            raise ValueError("Please provide api_key as first arg")
        main(sys.argv[-1])
```

    $ python best_moscow_streets.py <mos.ru-api-key>
    Street, points
     Тверская улица 49
     улица Земляной Вал 48
     Кутузовский проспект 46
     проспект Мира 45
     Комсомольский проспект 43


## Статистика оказаний услуг

     SELECT service_name, COUNT(*) FROM service_orders GROUP BY serice_name ORDER BY COUNT(*) DESC LIMIT 10;


## Обновление Python

    sudo add-apt-repository ppa:deadsnakes/ppa
    sudo apt-get update
    sudo apt-get install python3.8
    export p3path=$(which python3)
    sudo rm $p3path
    sudo ln -s $(which python3.8) $p3path
    python3 -V

PS. Менять дефолтную версию python на некоторых версиях Ubuntu может быть весьма плохой идеей, почти для любой цели лучше создать отдельный venv.

