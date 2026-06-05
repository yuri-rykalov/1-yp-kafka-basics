import json
import random
import time


class EnergyMeters:
    meter_id_min = 10000   # MIN energy meter ID
    meter_id_max = 99999   # MAX energy meter ID

class EnergyConsumption(EnergyMeters):
    consumpt_kwh_min = 1   # MIN consumption by 1 energy meter, kW/h
    consumpt_kwh_max = 400   # MAX consumption by 1 energy meter, kW/h

    def energy_cons_payload(self):
        """
        Generates energy consumption payload
        for 1 random energy meter
        Returns JSON
        """

        payload = {
            "meter_id": random.randint(self.meter_id_min, self.meter_id_max),
            "timestamp": int(time.time()),
            "consumpt_kwh": random.randint(self.consumpt_kwh_min, self.consumpt_kwh_max)
        }

        return payload
    
if __name__ == "__main__":
    nrg_cons = EnergyConsumption()

    for i in range(4):
        print(nrg_cons.energy_cons_payload())