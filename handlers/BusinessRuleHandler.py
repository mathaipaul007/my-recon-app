from connexion import request
from repository.AppcfgClientRepository import get_app_cfg_by_name, update_app_cfg

def read_rule():
    usersession = request.context["token_info"]
    clientid = usersession['clientid']

    appcfg = get_app_cfg_by_name(clientid, "RECON_CUSTOM_RULE", "RECON_CUSTOM_RULE")

    return appcfg.cfg_value

def update_rule(**kwargs):
    usersession = kwargs.get("token_info")
    print(usersession)

    body = kwargs.get("body", {})

    rule_data = body.get("rule_data")
    print(rule_data)
    updatedata = update_app_cfg(usersession["clientid"], "RECON_CUSTOM_RULE", "RECON_CUSTOM_RULE", rule_data)
    print(updatedata)

    return ""
