import json

# "description": "Biz: Require User id for agent activation; Tech: Pulling column from tb_agent table"


def read_json(path):
    with open(path) as f:
        return json.load(f)


def write_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f,indent=2)

import os
 
directory = 'D:\\repo\\copilot\\raw\\'
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if os.path.isfile(f) and filename.endswith('.json'):
        js = read_json(f) #'D:\\repo\\copilot\\demo.json')
        print(js)

        for i in js:
            if 'mode' not in i:
                i['mode'] = 'NULLABLE'
            if 'description' not in i:
                print('adding desc')
                print(f)
                i['description'] = 'Biz: Require ' + i['name'] + \
                    ' for agent activation; Tech: Pulling column from '+ filename.split('.')[0] +' table'
                print(len(i['description'].split(';')[0]),
                    len(i['description'].split(';')[0]))
                if len(i['description'].split(';')[0]) < 30 or len(i['description'].split(';')[1]) < 30:
                    print('invalid length')
        print(js)


        write_json('D:\\repo\\copilot\\processed\\'+filename, js)
