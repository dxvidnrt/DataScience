import mrjob
from collections.abc import Iterator, Iterable
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol, PickleValueProtocol


class BestMovies(MRJob):
    # Task: Find top 8 movies, average rating >= 4.0, |reviews| >= 10
    # Interested in MovieID, Reviews, Rating
    # Map1: Group by key
    # Reducer1: Calculate number reviews and total rating
    # Map2: Filter and avarage
    # Reducer2: Top 8

    # this pre-processing mapper is for convenience
    # it unpacks the string value into the logical signature of the file
    def map_pre(self, _: None, line: str) -> Iterator[tuple[int, tuple[str, str, float, str, float]]]:
        i, user_id, movie_id, rating, timestamp, rating_normalized = line.split(',')
        yield i, (user_id, movie_id, rating, timestamp, rating_normalized)

    def map_1(self, _: int, line: tuple[str, str, float, str, float]):
        # logical domain: N_0 x (U x M x R x T x R_N) -> (M x R)*
        user_id, movie_id, rating, timestamp, rating_normalized = line
        yield movie_id, rating
        

    def reduce_1(self, key, values):
        # logical domain: M x R* -> (M x (Q x N_0))*
        movie_id = key
        ratings = values
        total_rating = 0
        count = 0
        for rating in ratings:
            total_rating += float(rating)
            count += 1
        yield movie_id, (total_rating, count)

    def map_2(self, key, value):
        # logical domain: M x (Q x N_0) -> ({None}, (M x Q x N_0))*
        movie_id = key
        total_rating, count = value
        if count >= 10:
            average_rating = float(total_rating) / int(count)
            print(total_rating + " " + count + " " + average_rating)
            if average_rating >= 4.0:
                yield None, (movie_id, average_rating, count)
        
    def reduce_2(self, key, values):
        # logical domain: {None}, (M x Q x N_0)* -> ({None}, (M x Q))*
        movies_sorted = sorted(values, key=lambda x: x[1], reverse=True)
        for movie in movies_sorted[:8]:
            yield None, (movie[0], movie[1])

    # this post-processing mapper is for convenience
    # the output from the last reducer step is simply written as text into a file, ignoring the key
    def map_post(self, _: None, pair: tuple[str, float]):
        yield None, ','.join(map(str, pair))

    # the output from the last reducer step is simply written as text into a file, ignoring the key
    OUTPUT_PROTOCOL = TextValueProtocol

    # this can be treated as boilerplate
    def steps(self):
        return [MRStep(mapper=self.map_pre),
                MRStep(mapper=self.map_1, reducer=self.reduce_1),
                MRStep(mapper=self.map_2, reducer=self.reduce_2),
                MRStep(mapper=self.map_post)]


if __name__ == '__main__':
    BestMovies.run()
