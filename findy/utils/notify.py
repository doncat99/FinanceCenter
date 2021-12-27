from findy.vendor.wechatpy.work.client import WeChatClient

from findy import findy_config


def init_wechat_env():
    client = WeChatClient(findy_config['wechat_app_id'], findy_config['wechat_app_secrect'])
    agent = client.agent.get(findy_config['wechat_agent_id'])
    user_ids = agent.get('allow_userinfos').get('user')
    user_ids = [user.get('userid') for user in user_ids]
    return client, user_ids


def wechat_send(client, user_ids, type, msg):
    if type == 'markdown':
        client.message.send_markdown(findy_config['wechat_agent_id'], user_ids, msg)
    else:
        client.message.send_text(findy_config['wechat_agent_id'], user_ids, msg)


# from findy.utils.notify import init_wechat_env, wechat_send

# client, user_ids = init_wechat_env()

# type = 'markdown'
# stockname = 'AMD'
# timestamp = '09:00:00'
# action = 'Buy'
# price = 40.56
# amount = 99
# cost = price * amount
# text = f"# Stock: {stockname} \n > Time: {timestamp} \n > Action: <font color=\"warning\">{action}</font> \n > Price: <font color=\"comment\">{price}</font> \n > Amount: {amount}\n > Cost: <font color=\"info\">{cost}</font>"

# wechat_send(client, user_ids, type, text)
