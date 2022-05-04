from twisted.internet import reactor, threads


def get_val():
    i = 0
    while i < 100000000:
        i = i + 1
    print('returning from thread')
    return i


def user_message(result):
    print(result)


def create_deferred():
    deferred = threads.deferToThread(get_val)
    deferred.addCallback(user_message)

    user_message('Working bro.... wait')
    return deferred


# print(get_val())
create_deferred()
reactor.run()
# print('callback printing::'.format(deferred.callbacks))
