import sys
sys.path.append('./')
import gym
import numpy as np
from src.data.data_providers.roc_data_provider import *
from src.env.trading.virtual_trading_env import *
from stable_baselines.common.vec_env import  DummyVecEnv, VecCheckNan
from stable_baselines import PPO2

class RocMinEnv(gym.Env):
    metadata = {'render.modes': ['human', 'system', 'none']}
    def __init__(self, data_provider):
        self.__data_provider = data_provider
        self.__trading_stock_code = self.__data_provider.get_trading_stock_code()
        self.action_space = gym.spaces.Box(low = 0, high = 1,shape = (len(self.__trading_stock_code),))
        max_data, min_data = self.__data_provider.get_observation_max_min()
        print(max_data)
        print(min_data)
        self.__data_provider.reset_cycle()
        self.__data_provider.reset()
        self.observation_space = gym.spaces.Box(low=min_data, high=max_data, shape=self.__data_provider.get_observation_shape(), dtype=np.float16)

        self.__trading_env = {}
        for stock_code in self.__trading_stock_code:
            self.__trading_env[stock_code] = VirtualTradingEnv(100000, 0.1, stock_code, self.__data_provider)

    def reset(self):
        self.__data_provider.reset_cycle()
        self.__data_provider.reset()
        for trading_env in self.__trading_env.values():
            trading_env.reset()
        return self.__data_provider.get_observation()

    def step(self, action):
        for index, trading_env in enumerate(self.__trading_env.values()):
            trading_env.trading(action[index] > 0.5)
        reward = 0
        if self.__data_provider.is_last_data_of_the_day():
            for trading_env in self.__trading_env.values():
                reward += trading_env.get_income_roc()
                trading_env.reset()

        is_done = not self.__data_provider.move_next()
        return self.__data_provider.get_observation(), reward, is_done, {}

if __name__ == '__main__':
    tensorboard_folder = './tensorboard/'
    data_provider = RocDataProvider('/Users/kenneth/Desktop/Test', '2020-10-14 00:00:00', '2020-11-19 00:00:00', 10080, 'USDT')
    train_env = DummyVecEnv([lambda: RocMinEnv(data_provider)])
    train_env = VecCheckNan(train_env, raise_exception=True)
    model = PPO2('MlpPolicy', train_env, verbose=0, nminibatches=1, tensorboard_log=tensorboard_folder)
    model.learn(total_timesteps=2500000)
