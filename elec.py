def calc_electricity_cost(usage):
    # Electricity cost per kWh
    cost_per_kwh = 2.00
    
    # Inflation rate (% increase per year)
    inflation_rate = 3.0
    
    # Calculate total cost for 10 years
    cost = usage * cost_per_kwh * ((1 + (inflation_rate / 1000.0)) ** 10)
    return cost


# Usage per day in kWh
usage = 6
total_cost = calc_electricity_cost(usage)
print("Total cost for 10 years: £" + str(round(total_cost, 2)))