Отладка взаимодействия с Radist'ом
==================================

Иногда, при запуске скрипта, который использует *radist*, хочется узнать, что
происходит. Для этого используются переменные окружения **PYRADIST_DEBUG** и
**PYRADIST_FILE**

Установка обработчика
---------------------

Если задать переменную окружения **PYRADIST_DEBUG** как какое--нибудь число,
то в stderr будет выводиться информация о вызовах всех "внешних" процедур.

::

    $ PYRADIST_DEBUG=1 python2.4 utest.py IXFast2
    radist: debug mode enabled, unset PYRADIST_DEBUG if you don't want it
    radist func: IXConfig with args () {'config': '\nquick01.rambler.ru   -wwwFAST .........
    .
    ----------------------------------------------------------------------
    Ran 1 test in 0.006s

    OK
    $ python2.4 utest.py IXFast2
    .
    ----------------------------------------------------------------------
    Ran 1 test in 0.006s

    OK


На практике, только при запуске, происходит такое большое количество внутренних
процедур, что отладка становиться возможной только при отличном знании grep'а,
sed'а, awk'а и внутренностей radist'а. В большинстве случаев, достаточно
поставить **PYRADIST_DEBUG** в **helpers.r_exec**, что бы увидеть выполнение
всех команд на удаленных серверах:

::

    $ PYRADIST_DEBUG=helpers.r_exec python utest.py
    radist: debug mode enabled, unset PYRADIST_DEBUG if you don't want it
    radist func: r_exec with args ('10.1.1.1', 'hostname') {'stdin': <open file '/dev/null' ..........
    radist func: r_exec with args ('merger1.rambler.ru', 'hostname') {'stdin': <open file ' ..........
    radist func: r_exec with args ('index3.rambler.ru', 'hostname') {'stdin': <open file '/ ..........
    ...


**PYRADIST_DEBUG** может быть любое количество имен процедур, разделенных пробелами.

Обратите внимание, что если **PYRADIST_DEBUG** установить в `r_exec`, а не в
`helpers.r_exec`, то мы не увидим вызовов "внутри" radist'а, но увидим "внешние"
вызовы *radist.r_exec*.

::

    $ PYRADIST_DEBUG=r_exec python utest.py
    radist: debug mode enabled, unset PYRADIST_DEBUG if you don't want it
    ..............................................................
    ----------------------------------------------------------------------
    Ran 62 tests in 24.166s

    OK

Кроме *helpers.r_exec* могут быть интересны следующие значения:

  * radist.basenode.RadistNode.*любой нужный метод*;
  * radist.helpers.r_popen{2,3}
  * radist.alive.is_alive

Писать в...
-----------

По умолчанию, вывод идет в **stderr**. Часто этот вывод настолько велик, что
мешает видеть работу самого скрипта. В этом случае, рекомендую выставить
переменную окружения **PYRADIST_FILE** в нужное значение.
