import json
import Data


faquitas = open('Data\AequitasData.json')
falpha = open('Data\AlphaData.json')
ftsx = open('Data\TSXData.json')

aequitas = json.load(faquitas)
alpha = json.load(falpha)
tsx = json.load(ftsx)

faquitas.close()
falpha.close()
ftsx.close()

def prelim_data(exchange):
    message_types = []
    symbol_types = []
    # get all MessageType and Symbols from exchange
    for order in exchange:
        if order["MessageType"] not in message_types:
            message_types.append(order["MessageType"])
        if order["Symbol"] not in symbol_types:
            symbol_types.append(order["Symbol"])

    convo_direction = {}
    for message_type in message_types:
        convo_direction.update({message_type: []})
    for order in exchange:
        if order["Direction"] not in convo_direction[order["MessageType"]]:
            convo_direction[order["MessageType"]].append(order["Direction"])

    print(convo_direction)


def get_data(order_id):
    events = 0
    for order in aequitas:
        if order["OrderID"] == order_id:
            events = events + 1
            print(order)
    print(events)
            
def follow_trade_flow():
    trade_id = ''
    for order in tsx:
        if order["MessageType"] == "Trade":
            # print(order)
            trade_id = order["OrderID"]
            print(trade_id, '\n')
            return
    for order in aequitas:
        if order["OrderID"] == trade_id:
            print(order)

def create_workflows(exchange, exchange_name):
    
    workflow_by_id = {}

    for order in exchange:
        if order["OrderID"] not in workflow_by_id.keys():
            update = {order["OrderID"] : []}
            workflow_by_id.update(update)

    for order in exchange:
        workflow_by_id[order["OrderID"]].append(order["MessageType"])

    json_workflow = json.dumps(workflow_by_id, indent = 2, sort_keys=False)

    find_workflow_anomolies(workflow_by_id, exchange_name)
    with open(f'Data/Analysis/{exchange_name}.json', 'w') as file:
        file.write(json_workflow)

def find_workflow_anomolies(workflow, exchange_name):

    # Define which workflows are the expected lifecycle of a transaction.
    proper_workflows = [
                        ['NewOrderRequest','Rejected'],
                        ['NewOrderRequest', 'NewOrderAcknowledged', 'Trade'],
                        ['NewOrderRequest','NewOrderAcknowledged', 'CancelRequest', 'CancelAcknowledged', 'Cancelled'],
                        ['NewOrderRequest','NewOrderAcknowledged', 'CancelRequest','Cancelled'],
                        ]

    anomoly_objectid = {exchange_name:[]}
    
    for key, value in workflow.items():
        for workflow in proper_workflows:
            check = False
            if value == workflow:
                check = True
                break
            else:
                pass
        if check == False:
            anomoly_objectid[exchange_name].append(key)
    with open(f'Data/Analysis/Anomolies/{exchange_name}.json', 'w') as file:
        file.write(json.dumps(anomoly_objectid, indent = 2, sort_keys=False))

            
    
    print(anomoly_objectid)

# Make sure the market doesnt have pre market trading hours before running
def check_postorpre_market_open(exchange, exchange_name):
    post_open = []
    pre_open = []
    lifecycle = json.load(open(f'Data/Analysis/{exchange_name}.json', 'r'))
    


    for order in exchange:
        if int(order["TimeStampEpoch"]) >= 167301540000000:
            post_open.append(order["OrderID"])
        else:
            pre_open.append(order["OrderID"])
    
    print(pre_open)


    print(len(post_open))
    for key, values in lifecycle.items():
        for x in pre_open:
            if key == x:
                print(lifecycle[key][len(lifecycle[key]) - 1])


# print('Alpha conversation Direction')
# prelim_data(alpha)
# # get_data('b8c529d1-9283-11ed-9f49-047c16291a22')
# follow_trade_flow()

check_postorpre_market_open(tsx, 'tsx')
# create_workflows(tsx, 'tsx')
# create_workflows(aequitas, 'aequitas')
# create_workflows(alpha, 'alpha')