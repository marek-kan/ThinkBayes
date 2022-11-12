import pandas as pd
import concurrent.futures as cf
import time
from multiprocessing import cpu_count

def calc_on_group(g):
    def calc_resplacement2(x, _group, max_idx):
        lifetime = x[lifetime_col]
        idx = x.name
        new_idx = idx + lifetime
        if new_idx <= max_idx:
            growth = x[increment_col]
            replaced = _group.loc[idx]['replaced'] 
            _group.loc[new_idx, 'replaced'] = growth+replaced
    m_idx = g[1].index.max()
    g[1].apply(lambda x: calc_resplacement2(x, g[1], m_idx), axis=1)
    return g[1]

# df_o = pd.read_csv('/home/marek/Downloads/sales_and_replacement_charger_lifetime_merged-Germany.csv', delimiter=';',thousands=',')
print('reading data')
df = pd.read_parquet('/home/marek/Downloads/Sales_10M.parquet')#.head(10000)
gb_cols = ['Optimization scenario', 'Scenario', 'Country', 'State', 'Vehicle Type',
    'Archetypes', 'Powertrain', 'Vehicle segment', 'Location',
    'Charger technology']
print('changing dtype')
df[gb_cols] = df[gb_cols].astype(str).fillna('XNA') # filled with string minor improvement

print('length of data:', len(df))
increment_col = 'Growth Chargers'
lifetime_col = 'Charger lifetime'
df['replaced'] = 0

pool = int(cpu_count())
print('grouping')
g1 = df.groupby(gb_cols)
print('pool:', pool)

if __name__ == '__main__':

    start = time.time()
    print('start')
    with cf.ProcessPoolExecutor(pool) as p:
        r = list(p.map(calc_on_group, g1))
        
    print('len R:', len(r))
    print('concat')
    
    res = pd.concat(r)

    print(res)
    print('len data:', len(df))
    print('len res:', len(res))
    print('exec time:', time.time()-start)