class MoEngageUserPropertyDTO:
    
    def __init__(self, user_id: int, first_campaign: str, first_network: str):
        self.user_id = user_id
        self.signup_event = 'Signup_stage1'
        self.first_campaign = first_campaign
        self.first_network = first_network

    def to_moengage_dict(self):
        return dict(
            type="customer",
            customer_id=self.user_id,
            attributes={
                "First Campaign": self.first_campaign,
                "First Network": self.first_network
            }
        )

    def to_bnxt_dict(self):
        return dict(
            eventName='Adjust_Events',
            attribute={
                "customerId": self.user_id,
                "Event": self.signup_event,
                "Campaign": self.first_campaign,
                "Network": self.first_network
            },
            userProperty={
                "First Campaign": self.first_campaign,
                "First Network": self.first_network
            }
        )


class MoEngageEventDTO:

    def __init__(self, user_id: int, event: str, event_campaign: str, event_network: str):
        self.user_id = user_id
        self.event = event
        self.event_campaign = event_campaign
        self.event_network = event_network

    def to_moengage_dict(self):
        return dict(
            type="event",
            customer_id=self.user_id,
            actions=[
                dict(
                    action="Adjust_Events",
                    attributes={
                        "Event": self.event,
                        "Campaign": self.event_campaign,
                        "Network": self.event_network
                    }
                )
            ]
        )

    def to_bnxt_dict(self):
        return dict(
            eventName='Adjust_Events',
            attribute={
                "customerId": self.user_id,
                "Event": self.event,
                "Campaign": self.event_campaign,
                "Network": self.event_network
            },
            userProperty=None
        )