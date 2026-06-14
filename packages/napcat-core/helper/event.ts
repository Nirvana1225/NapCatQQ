import { NodeIQQNTWrapperSession } from '@/napcat-core/wrapper';
import { randomUUID } from 'crypto';
import { ListenerNamingMapping, ServiceNamingMapping } from '@/napcat-core/index';

interface InternalMapKey {
  timeout: number;
  createtime: number;
  func: (...arg: any[]) => any;
  checker: ((...args: any[]) => boolean) | undefined;
}

type EnsureFunc<T> = T extends (...args: any) => any ? T : never;

type FuncKeys<T> = Extract<
  {
    [K in keyof T]: EnsureFunc<T[K]> extends never ? never : K;
  }[keyof T],
  string
>;

export type ListenerClassBase = Record<string, string>;

export class NTEventWrapper {
  private readonly WrapperSession: NodeIQQNTWrapperSession | undefined; // WrapperSession
  private readonly listenerManager: Map<string, ListenerClassBase> = new Map<string, ListenerClassBase>(); // ListenerName-Unique -> Listener实例
  private readonly EventTask = new Map<string, Map<string, Map<string, InternalMapKey>>>(); // tasks ListenerMainName -> ListenerSubName-> uuid -> {timeout,createtime,func}

  constructor (
    wrapperSession: NodeIQQNTWrapperSession
  ) {
    this.WrapperSession = wrapperSession;

    // [FIX] 定时清理过期 EventTask 条目，防止内存泄漏
    // 原实现仅在事件触发时清理，如果事件长期不触发则条目永不释放
    const cleanupTimer = setInterval(() => {
      const now = Date.now();
      let cleaned = 0;
      for (const [mainName, subMap] of this.EventTask) {
        for (const [subName, taskMap] of subMap) {
          for (const [uuid, task] of taskMap) {
            if (task.createtime + task.timeout < now) {
              taskMap.delete(uuid);
              cleaned++;
            }
          }
          // 清理空的子 Map
          if (taskMap.size === 0) {
            subMap.delete(subName);
          }
        }
        // 清理空的主 Map
        if (subMap.size === 0) {
          this.EventTask.delete(mainName);
        }
      }
      if (cleaned > 0) {
        // 仅在清理了条目时输出日志，避免刷屏
        // console.log(`[NTEventWrapper] Cleaned ${cleaned} expired EventTask entries`);
      }
    }, 60000); // 每分钟清理一次
    cleanupTimer.unref(); // 不阻止进程退出
  }

  createProxyDispatch (ListenerMainName: string) {
    const dispatcherListenerFunc = this.dispatcherListener.bind(this);
    return new Proxy(
      {},
      {
        get (target: any, prop: any, receiver: any) {
          if (typeof target[prop] === 'undefined') {
            // 如果方法不存在，返回一个函数，这个函数调用existentMethod
            return (...args: any[]) => {
              dispatcherListenerFunc(ListenerMainName, prop, ...args).then();
            };
          }
          // 如果方法存在，正常返回
          return Reflect.get(target, prop, receiver);
        },
      }
    );
  }

  createEventFunction<
    Service extends keyof ServiceNamingMapping,
    ServiceMethod extends FuncKeys<ServiceNamingMapping[Service]>,
    T extends (...args: any) => any = EnsureFunc<ServiceNamingMapping[Service][ServiceMethod]>
  >(eventName: `${Service}/${ServiceMethod}`): T | undefined {
    const eventNameArr = eventName.split('/');
    type eventType = {
      [key: string]: () => { [key: string]: (...params: Parameters<T>) => Promise<ReturnType<T>>; };
    };
    if (eventNameArr.length > 1) {
      const serviceName = 'get' + (eventNameArr[0]?.replace('NodeIKernel', '') ?? '');
      const eventName = eventNameArr[1];
      const services = (this.WrapperSession as unknown as eventType)[serviceName]?.();
      if (!services || !eventName) {
        return undefined;
      }
      let event = services[eventName];

      // 重新绑定this
      event = event?.bind(services);
      if (event) {
        return event as T;
      }
      return undefined;
    }
    return undefined;
  }

  createListenerFunction<T> (listenerMainName: string, uniqueCode: string = ''): T {
    const existListener = this.listenerManager.get(listenerMainName + uniqueCode);
    if (!existListener) {
      const Listener = this.createProxyDispatch(listenerMainName);
      const ServiceSubName = /^NodeIKernel(.*?)Listener$/.exec(listenerMainName)![1];
      const Service = `NodeIKernel${ServiceSubName}Service/addKernel${ServiceSubName}Listener`;

      // @ts-ignore
      this.createEventFunction(Service)(Listener as T);
      this.listenerManager.set(listenerMainName + uniqueCode, Listener);
      return Listener as T;
    }
    return existListener as T;
  }

  // 统一回调清理事件
  async dispatcherListener (ListenerMainName: string, ListenerSubName: string, ...args: any[]) {
    this.EventTask.get(ListenerMainName)
      ?.get(ListenerSubName)
      ?.forEach((task, uuid) => {
        if (task.createtime + task.timeout < Date.now()) {
          this.EventTask.get(ListenerMainName)?.get(ListenerSubName)?.delete(uuid);
          return;
        }
        if (task?.checker?.(...args)) {
          task.func(...args);
        }
      });
  }

  async callNoListenerEvent<
    Service extends keyof ServiceNamingMapping,
    ServiceMethod extends FuncKeys<ServiceNamingMapping[Service]>,
    EventType extends (...args: any) => any = EnsureFunc<ServiceNamingMapping[Service][ServiceMethod]>
  >(
    serviceAndMethod: `${Service}/${ServiceMethod}`,
    ...args: Parameters<EventType>
  ): Promise<Awaited<ReturnType<EventType>>> {
    return (this.createEventFunction(serviceAndMethod))!(...args);
  }

  async registerListen<
    Listener extends keyof ListenerNamingMapping,
    ListenerMethod extends FuncKeys<ListenerNamingMapping[Listener]>,
    ListenerType extends (...args: any) => any = EnsureFunc<ListenerNamingMapping[Listener][ListenerMethod]>
  >(
    listenerAndMethod: `${Listener}/${ListenerMethod}`,
    checker: (...args: Parameters<ListenerType>) => boolean,
    waitTimes = 1,
    timeout = 5000
  ) {
    return new Promise<Parameters<ListenerType>>((resolve, reject) => {
      const ListenerNameList = listenerAndMethod.split('/');
      const ListenerMainName = ListenerNameList[0] ?? '';
      const ListenerSubName = ListenerNameList[1] ?? '';
      const id = randomUUID();
      let complete = 0;
      let retData: Parameters<ListenerType> | undefined;

      function sendDataCallback () {
        if (complete === 0) {
          reject(new Error(' ListenerName:' + listenerAndMethod + ' timeout'));
        } else {
          resolve(retData!);
        }
      }

      const timeoutRef = setTimeout(sendDataCallback, timeout);
      const eventCallback = {
        timeout,
        createtime: Date.now(),
        checker,
        func: (...args: Parameters<ListenerType>) => {
          complete++;
          retData = args;
          if (complete >= waitTimes) {
            clearTimeout(timeoutRef);
            sendDataCallback();
          }
        },
      };
      if (!this.EventTask.get(ListenerMainName)) {
        this.EventTask.set(ListenerMainName, new Map());
      }
      if (!this.EventTask.get(ListenerMainName)?.get(ListenerSubName)) {
        this.EventTask.get(ListenerMainName)?.set(ListenerSubName, new Map());
      }
      this.EventTask.get(ListenerMainName)?.get(ListenerSubName)?.set(id, eventCallback);
      this.createListenerFunction(ListenerMainName);
    });
  }

  async callNormalEventV2<
    Service extends keyof ServiceNamingMapping,
    ServiceMethod extends FuncKeys<ServiceNamingMapping[Service]>,
    Listener extends keyof ListenerNamingMapping,
    ListenerMethod extends FuncKeys<ListenerNamingMapping[Listener]>,
    EventType extends (...args: any) => any = EnsureFunc<ServiceNamingMapping[Service][ServiceMethod]>,
    ListenerType extends (...args: any) => any = EnsureFunc<ListenerNamingMapping[Listener][ListenerMethod]>
  >(
    serviceAndMethod: `${Service}/${ServiceMethod}`,
    listenerAndMethod: `${Listener}/${ListenerMethod}`,
    args: Parameters<EventType>,
    checkerEvent: (ret: Awaited<ReturnType<EventType>>) => boolean = () => true,
    checkerListener: (...args: Parameters<ListenerType>) => boolean = () => true,
    callbackTimesToWait = 1,
    timeout = 5000
  ) {
    const id = randomUUID();
    let complete = 0;
    let retData: Parameters<ListenerType> | undefined;
    let retEvent: any = {};

    function sendDataCallback (resolve: any, reject: any) {
      if (complete === 0) {
        reject(
          new Error(
            'Timeout: NTEvent serviceAndMethod:' +
            serviceAndMethod +
            ' ListenerName:' +
            listenerAndMethod +
            ' EventRet:\n' +
            JSON.stringify(retEvent, null, 4) +
            '\n'
          )
        );
      } else {
        resolve([retEvent as Awaited<ReturnType<EventType>>, ...retData!]);
      }
    }

    const ListenerNameList = listenerAndMethod.split('/');
    const ListenerMainName = ListenerNameList[0] ?? '';
    const ListenerSubName = ListenerNameList[1] ?? '';

    return new Promise<[EventRet: Awaited<ReturnType<EventType>>, ...Parameters<ListenerType>]>(
      (resolve, reject) => {
        const timeoutRef = setTimeout(() => sendDataCallback(resolve, reject), timeout);

        const eventCallback = {
          timeout,
          createtime: Date.now(),
          checker: checkerListener,
          func: (...args: any[]) => {
            complete++;
            retData = args as Parameters<ListenerType>;
            if (complete >= callbackTimesToWait) {
              clearTimeout(timeoutRef);
              sendDataCallback(resolve, reject);
            }
          },
        };
        if (!this.EventTask.get(ListenerMainName)) {
          this.EventTask.set(ListenerMainName, new Map());
        }
        if (!this.EventTask.get(ListenerMainName)?.get(ListenerSubName)) {
          this.EventTask.get(ListenerMainName)?.set(ListenerSubName, new Map());
        }
        this.EventTask.get(ListenerMainName)?.get(ListenerSubName)?.set(id, eventCallback);
        this.createListenerFunction(ListenerMainName);

        const eventResult = this.createEventFunction(serviceAndMethod)!(...(args));

        const eventRetHandle = (eventData: any) => {
          retEvent = eventData;
          if (!checkerEvent(retEvent) && timeoutRef.hasRef()) {
            clearTimeout(timeoutRef);
            reject(
              new Error(
                'EventChecker Failed: NTEvent serviceAndMethod:' +
                serviceAndMethod +
                ' ListenerName:' +
                listenerAndMethod +
                ' EventRet:\n' +
                JSON.stringify(retEvent, null, 4) +
                '\n'
              )
            );
          }
        };
        if (eventResult instanceof Promise) {
          eventResult.then((eventResult: any) => {
            eventRetHandle(eventResult);
          })
            .catch(reject);
        } else {
          eventRetHandle(eventResult);
        }
      }
    );
  }
}
