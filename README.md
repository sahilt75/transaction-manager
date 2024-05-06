# transaction-manager

#### Rules for withdrawal 
1. The withdraw amount has to be subtracted from the balance, always considering the
balance_order in ascending order.
1. If a balance value is not enough, you should check if the account_id has more
than one balance at its disposal and withdraw the rest from another balance in
the same account_id.
2. If the withdraw value is greater than the sum of all balance values left in the
account, the withdraw should not happen.
3. If a balance in the account reaches 0, it's status should be changed to "BALANCE
WITHDREW".
4. The initial_balance has to have the initial value before the withdraw, and the
available_balance has to reflect how much was left after the withdraws.
5. validation_result column should show if the withdraw happened or not. It'll be
a plus if you specify what happened with that withdraw.

## Steps to run the script

#### 1. Create a virtual env
```bash
$ virtualenv -p python3 venv
```
##### Activate it
```bash
$ source venv/bin/activate
```

#### 2. Install deps
```bash
$ pip install -r requirements.txt
```

#### 3. Run App
```bash
$ python main.py
```

#### Tests
```bash
$ pytest
```