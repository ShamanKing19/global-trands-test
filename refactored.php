<?php

use Illuminate\Queue\InteractsWithQueue;

class ProcessIncomingCallJob implements ShouldQueue
{
    use InteractsWithQueue;

    public $backoff = [10, 30, 60, 120];

    private $callId;

    public function __construct($callId)
    {
        $this->callId = $callId;
    }

    public function handle()
    {
        [$call, $operator] = DB::transaction(function () {
            $call = Call::lockForUpdate()->find($this->callId);
            if (!$call) {
                return [null, null];
            }

            // Статусы лучше вынести в Enum (или в константы, если версия php ниже 8.1)
            if ($call->status !== 'new') {
                return [null, null];
            }

            $client = Client::where('phone', $call->phone)->select(['id'])->first();
            if ($client) {
                $call->client_id = $client->id;
            }

            // Для быстрой отработки запроса и избежания блокировки всей таблицы нужен индекс по (last_call_at, available)
            $operator = Operator::lock('FOR UPDATE SKIP LOCKED')
                ->where('available', true)
                ->orderBy('last_call_at')
                ->first();

            if (!$operator) {
                Log::notice('No free operators', [
                    'call_id' => $this->callId,
                    'attempt' => $this->attempts(),
                ]);
                $this->release(30);
                return [null, null];
            }

            // Поле last_call_at скорее всего будет обновляться в другом хэндлере. Если нет, то можно обновить его тут.
            $operator->available = false;
            $operator->save();

            $call->operator_id = $operator->id;
            $call->status = 'assigned';
            $call->save();

            Log::info('Call assigned', [
                'call_id' => $call->id,
                'operator_id' => $operator->id,
                'attempt' => $this->attempts(),
            ]);

            return [$call, $operator];
        });

        if (!isset($call, $operator)) {
            return;
        }

        try {
            // Подразумеваем, что в случае ошибки всплывёт специфичное исключение.
            // Если бы возвращался какой-нибудь response, то проверять можно было бы его.
            app(TelephonyClient::class)->sendCallAssigned($call->id, $operator->id);
        } catch (TelephonyException $e) {
            DB::transaction(function () use ($call, $operator) {
                $operator->available = true;
                $operator->save();
                $call->status = 'new';
                $call->save();
            });
            Log::error('Api error', [
                'call_id' => $call->id,
                'operator_id' => $operator->id,
                'attempt' => $this->attempts(),
                'error' => $e->getMessage(),
            ]);
            throw new \RuntimeException('Api error', previous: $e);
        } catch (Throwable $e) {
            Log::error('Api unhandled error', [
                'call_id' => $call->id,
                'operator_id' => $operator->id,
                'attempt' => $this->attempts(),
                'error' => $e->getMessage(),
            ]);
            throw new \RuntimeException('Api unhandled error', previous: $e);
        }
    }

    // Вместо фиксированного значения tries
    public function retryUntil(): \DateTime
    {
        return now()->addMinutes(30);
    }

    public function failed(\Throwable $e): void
    {
        Log::error('Call processing failed', [
            'call_id' => $this->callId,
            'error' => $e->getMessage(),
        ]);

        // Помечаем звонок как failed, чтобы он не завис в неизвестном статусе
        Call::where('id', $this->callId)->update(['status' => 'failed']);
    }
}
