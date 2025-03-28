import Bottleneck from "@sergiiivzhenko/bottleneck";

export function BottleneckDecorator(getOptions?: (...args: never[]) => Bottleneck.JobOptions): MethodDecorator {
    return function (self: object, _propertyName: string | symbol, descriptor: PropertyDescriptor): void {
        const original = descriptor.value;
        const className = self.constructor.name;

        descriptor.value = async function (this: { limiter: Bottleneck }, ...args: never[]): Promise<unknown> {
            const limiter = this.limiter;
            if (!limiter) {
                throw new Error(`${className} Bottleneck limiter is missing`);
            }
            //debug(`BottleneckDecorator ${className} ${propertyName} ${JSON.stringify(args)}`);
            const options = getOptions ? getOptions(...args) : {};

            const wrapped = limiter.wrap(() => original.call(this, ...args));
            return wrapped.withOptions({ expiration: 30_000, ...options });
        };
    };
}
