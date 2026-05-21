<?php

class ProcessIncomingCallJob implements ShouldQueue
{
    // Лучше не просто указать количество попыток, а сделать exponential backoff для ретраев,
    // т. к. оператор может освободиться не сразу, а через n минут. К тому же ошибки могут быть разные и не для всех
    // нужно "использовать" попытки.
    public $tries = 5;

    private $callId;

    public function __construct($callId)
    {
        $this->callId = $callId;
    }

    public function handle()
    {
        // Логику обновления записей стоит обернуть в транзакцию, а саму запись блокировать для избежания повторной обработки
        $call = Call::find($this->callId);

        if (!$call) {
            return;
        }

        // Стоит использовать enum или константы вместо "магических" значений
        if ($call->status === 'new') {
            // нам нужен только id клиента, поэтому стоит выбрать его, а не все поля
            $client = Client::where('phone', $call->phone)->first();
            if ($client) {
                $call->client_id = $client->id;
            }

            // race-condition: разные воркеры могут выбрать одного и того же оператора.
            // Нужна блокировка FOR UPDATE SKIP LOCKED.
            $operator = Operator::where('available', true)
                ->orderBy('last_call_at')
                ->first();

            if (!$operator) {
                // Из-за простого исключения расходуем попытки, хотя нужно просто "попробовать попозже"
                throw new \Exception('No available operators');
            }

            $operator->available = false;
            $operator->save();

            $call->operator_id = $operator->id;
            $call->status = 'assigned';
            $call->save();

            // Нужно обрабатывать ответ. В случае неудачи нужно откатить транзакцию. При успехе же транзакцию коммитим.

            // HTTP-запрос во внешнюю телефонию для назначения звонка оператору.
            // Гарантии внешней системы неизвестны.
            app(TelephonyClient::class)->sendCallAssigned($call->id, $operator->id);

            Log::info('Call assigned', [
                'call_id' => $call->id,
                'operator_id' => $operator->id,
            ]);
        }
    }
}
