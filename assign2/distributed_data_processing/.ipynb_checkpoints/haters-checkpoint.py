import mrjob
from collections.abc import Iterator, Iterable
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol


class Haters(MRJob):
    """
    Why it would be nice to have combiners for this task:
    Using a combiner to accumelate all ratings below 2 starts, minimizes the communication between mapper and reducer.
    In a best case scenario, a mapper maps all ratings of the same user, effectively reducing the network traffic from |ratings below 2| to     1.
    """


    def mapper(self, _, line: str):
        i, user_id, movie_id, rating, timestamp, rating_normalized = line.split(',')
        if float(rating) < 2:
            yield user_id, 1

    def reducer(self, key, values):
        user_id = key
        if sum(values) > 50:
            yield None, user_id


if __name__ == '__main__':
    Haters.run()
