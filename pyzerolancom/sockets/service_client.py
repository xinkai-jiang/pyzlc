class ServiceProxy:
    @staticmethod
    def request(
        service_name: str,
        request_encoder: Callable[[RequestT], bytes],
        response_decoder: Callable[[bytes], ResponseT],
        request: RequestT,
    ) -> Optional[ResponseT]:
        if LanComNode.instance is None:
            raise ValueError("Lancom Node is not initialized")
        node = LanComNode.instance
        service_component = node.nodes_map.get_service_info(service_name)
        if service_component is None:
            logger.warning(f"Service {service_name} is not exist")
            return None
        request_bytes = request_encoder(request)
        addr = f"tcp://{service_component['ip']}:{service_component['port']}"
        response = node.loop_manager.submit_loop_task(
            send_bytes_request(addr, service_name, request_bytes),
            True,
        )
        return response_decoder(cast(bytes, response))