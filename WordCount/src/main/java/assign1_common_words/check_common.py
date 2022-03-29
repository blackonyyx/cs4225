import operator
import sys


def mark(answer_path, output_path):
    try:
        count = []
        word = []
        with open(answer_path, 'r') as answer:
            for line in answer:
                params = line.split()
                count.append(int(params[0]))
                word.append(params[1])

        with open(output_path, 'r') as output:
            i = 0
            for line in output:
                if i >= 20:
                    return -1
                params = line.split()
                local_count = int(params[0])
                local_word = params[1]
                if count[i] != local_count or word[i] != local_word:
                    return -1
                i += 1
            if i != 20:
                return -1
            return 1
    except BaseException as error:
        print('An exception occurred: {}'.format(error))
        return -1

if __name__ == '__main__':
    result = mark(sys.argv[1], sys.argv[2])
    if result == 1:
        print("Test Passed")
    elif result <= 0:
        print("Wrong Answer")
