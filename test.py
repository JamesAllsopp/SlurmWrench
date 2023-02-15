from multiprocessing import Pool
from get_next_block import get_simulations
def f():
    return "bah!"

if __name__ == '__main__':
    results=[]
    with Pool() as pool:
        future_results = [pool.apply_async(f) for i in range(2)]
        results = [f.get() for f in future_results]
    print(results)
