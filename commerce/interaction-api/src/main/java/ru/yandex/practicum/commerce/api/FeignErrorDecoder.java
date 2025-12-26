package ru.yandex.practicum.commerce.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import feign.Response;
import feign.codec.ErrorDecoder;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.commerce.dto.ErrorDto;
import ru.yandex.practicum.commerce.exception.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Slf4j
public class FeignErrorDecoder implements ErrorDecoder {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ErrorDecoder defaultDecoder = new Default();

    @Override
    public Exception decode(String methodKey, Response response) {
        ErrorDto errorDto = null;

        try {
            if (response.body() != null) {
                errorDto = parseErrorResponse(response.body().asInputStream());
            }
        } catch (IOException e) {
            log.warn("Не удалось разобрать тело ответа об ошибке для метода: {}", methodKey, e);
        }

        int status = response.status();
        String userMessage = errorDto != null && errorDto.getUserMessage() != null
                ? errorDto.getUserMessage()
                : "Произошла ошибка при обращении к сервису";

        Exception exception = mapStatusCodeToException(status, userMessage, methodKey);

        if (exception != null) {
            return exception;
        }

        return defaultDecoder.decode(methodKey, response);
    }

    private ErrorDto parseErrorResponse(InputStream body) throws IOException {
        try {
            String bodyString = new String(body.readAllBytes(), StandardCharsets.UTF_8);
            return objectMapper.readValue(bodyString, ErrorDto.class);
        } catch (Exception e) {
            log.debug("Сообщение об ошибке не в формате ErrorDto, игнорируется", e);
            return null;
        }
    }

    private Exception mapStatusCodeToException(int status, String userMessage, String methodKey) {
        return switch (status) {
            case 400 -> new BadRequestException(userMessage);
            case 401 -> new UnauthorizedException(userMessage);
            case 403 -> new ForbiddenException(userMessage);
            case 404 -> new NotFoundException(userMessage);
            case 409 -> new ConflictException(userMessage);
            case 422 -> new ValidationException(userMessage);
            case 500, 502, 503, 504 -> new UpstreamServiceException(
                    String.format("Ошибка внешнего сервиса [%s]: %s", methodKey, userMessage));
            default -> null;
        };
    }
}